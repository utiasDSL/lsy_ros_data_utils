// Copyright (c) 2026  Learning Systems and Robotics Lab,
// Technical University of Munich (TUM)
//
// Authors:  Haoming Zhang <haoming.zhang@tum.de>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

//
// Created by haoming on 1/17/26.
//

#include "bag_recorder_component.hpp"

#include <chrono>
#include <cstring>
#include <stdexcept>
#include <filesystem>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <ctime>
#include <type_traits>
#include <utility>
#include <yaml-cpp/yaml.h>

#include <rclcpp/serialization.hpp>

#include <rosbag2_cpp/converter_options.hpp>
#include <rosbag2_storage/storage_options.hpp>
#include <rosbag2_storage/serialized_bag_message.hpp>
#include <ament_index_cpp/get_package_share_directory.hpp>

// Prefer new API (send_timestamp/recv_timestamp) if it exists
template<typename M>
auto set_ts_impl(M &m, int64_t send_ns, int64_t recv_ns, int)
  -> decltype((m.send_timestamp = send_ns, m.recv_timestamp = recv_ns), void()) {
  m.send_timestamp = send_ns;
  m.recv_timestamp = recv_ns;
  return;
}

// Fallback to old API (time_stamp) if it exists
template<typename M>
auto set_ts_impl(M &m, int64_t send_ns, int64_t /*recv_ns*/, long)
  -> decltype((m.time_stamp = send_ns), void()) {
  m.time_stamp = send_ns;
  return;
}

namespace lsy_ros_data_utils::rosbag {
  // ---------------- helpers ----------------
  // Detect member: time_stamp
  void BagRecorderComponent::set_bag_message_timestamps(
    rosbag2_storage::SerializedBagMessage &m,
    int64_t send_ns,
    int64_t recv_ns) const {
    // int overload is preferred; if not viable, falls back to long overload.
    // If neither is viable, you'll get a clear compile error here.
    set_ts_impl(m, send_ns, recv_ns, 0);
  }

  static bool should_warn(std::atomic<int64_t> &last_ns,
                          int64_t now_ns,
                          double period_sec) {
    const int64_t period_ns = static_cast<int64_t>(period_sec * 1e9);
    int64_t prev = last_ns.load(std::memory_order_relaxed);
    if (now_ns - prev < period_ns) return false;
    return last_ns.compare_exchange_strong(prev, now_ns, std::memory_order_relaxed);
  }


  // ---------------- main ----------------

  BagRecorderComponent::BagRecorderComponent(const rclcpp::NodeOptions &node_options)
    : rclcpp::Node("bag_recorder_component", ensureIntraProcesComm(node_options)) {
    this->declare_parameter<std::string>("config_file", "");
    cfg_file_ = this->get_parameter("config_file").as_string();
    if (cfg_file_.empty()) {
      throw std::runtime_error("Parameter 'config_file' is required.");
    }
    this->init_type_registry();
    this->load_config(cfg_file_);

    // open bags + create topics
    for (auto &br: bags_) {
      this->open_bag_and_create_topics(*br);
    }

    // one subscription per unique topic, created from type string via registry
    this->create_subscriptions();

    // writer thread per bag
    for (auto &br: bags_) {
      auto *raw = br.get();
      raw->thread = std::thread([this, raw]() { this->writer_loop(raw); });
    }

    if (monitor_cfg_.enabled && monitor_cfg_.print_hz > 0.0) {
      const auto period = std::chrono::duration<double>(1.0 / monitor_cfg_.print_hz);
      monitor_timer_ = this->create_wall_timer(
        std::chrono::duration_cast<std::chrono::nanoseconds>(period),
        [this]() { this->monitor_timer_cb(); });
    }

    RCLCPP_INFO(get_logger(),
                "BagRecorderComponent started. bags=%zu unique_topics=%zu",
                bags_.size(), topic_to_type_.size());
  }

  BagRecorderComponent::~BagRecorderComponent() {
    is_shutting_down_.store(true);

    subs_.clear();

    // 3. Wait for all currently executing ghost callbacks to physically exit
    // while (active_callbacks_.load() > 0) {
    //   std::this_thread::sleep_for(std::chrono::milliseconds(1));
    // }

    for (const auto &br: bags_) {
      br->stop.store(true);
      br->cv.notify_all();
    }

    for (const auto &br: bags_) {
      if (br->thread.joinable()) {
        br->thread.join();
      }
    }


    RCLCPP_INFO(get_logger(), "BagRecorderComponent destroying, waiting for 5 seconds.");
    std::this_thread::sleep_for(std::chrono::seconds(5));
    RCLCPP_INFO(get_logger(), "BagRecorderComponent destroyed.");
  }

  void
  BagRecorderComponent::load_config(const std::string &yaml_path) {
    YAML::Node config = YAML::LoadFile(yaml_path);
    if (!config["bags"] || !config["bags"].IsSequence()) {
      throw std::runtime_error("YAML must contain 'bags' as a sequence.");
    }

    // callback groups
    if (config["num_callback_groups"]) {
      const auto num_callback_groups = config["num_callback_groups"].as<size_t>();
      for (size_t i = 0; i < num_callback_groups; ++i) {
        cb_groups_.push_back(
          this->create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive));
      }
    } else {
      throw std::runtime_error("Parameter 'num_callback_groups' required!");
    }

    // std::cout << config << std::endl;
    // monitor (optional)
    monitor_cfg_ = MonitorCfg{};
    if (config["monitor"]) {
      const auto m = config["monitor"];
      monitor_cfg_.enabled = m["enabled"] ? m["enabled"].as<bool>() : true;
      monitor_cfg_.print_hz = m["print_hz"] ? m["print_hz"].as<double>() : 1.0;
      monitor_cfg_.ewma_tau_sec = m["ewma_tau_sec"] ? m["ewma_tau_sec"].as<double>() : 2.0;
      monitor_cfg_.warn_grace_sec = m["warn_grace_sec"] ? m["warn_grace_sec"].as<double>() : 2.0;
      monitor_cfg_.use_ansi_color = m["use_ansi_color"] ? m["use_ansi_color"].as<bool>() : true;
    }

    bags_.clear();

    for (const auto &bag: config["bags"]) {
      auto br = std::make_unique<BagRuntime>();
      br->spec.name = require_string(bag, "name");
      br->spec.bag_uri = require_string(bag, "bag_uri");
      br->spec.storage_id = bag["storage_id"] ? bag["storage_id"].as<std::string>() : std::string("mcap");

      br->spec.max_queue_size = bag["max_queue_size"] ? bag["max_queue_size"].as<size_t>() : 20000;
      br->spec.max_queue_bytes = bag["max_queue_bytes"]
                                   ? bag["max_queue_bytes"].as<size_t>()
                                   : (1024ull * 1024 * 1024); // 1 GiB default

      br->spec.bag_cache_size = bag["bag_cache_size"]
                                  ? bag["bag_cache_size"].as<size_t>() * 1024 * 1024 * 1024
                                  : (2048ull * 1024 * 1024); // 1 GiB default

      br->spec.max_bag_size = bag["max_bag_size"]
                                ? bag["max_bag_size"].as<size_t>() * 1024 * 1024 * 1024
                                : (50ull * 1024 * 1024 * 1024); // 1 GiB default

      br->spec.overflow_policy = bag["overflow_policy"] && bag["overflow_policy"].as<std::string>() == "block"
                                   ? BagSpec::OverflowPolicy::BLOCK
                                   : BagSpec::OverflowPolicy::DROP_OLDEST;

      br->spec.warn_on_overflow = bag["warn_on_overflow"] ? bag["warn_on_overflow"].as<bool>() : true;
      br->spec.overflow_warn_period_sec =
          bag["overflow_warn_period_sec"] ? bag["overflow_warn_period_sec"].as<double>() : 1.0;


      br->spec.uri_with_datetime = bag["uri_with_datetime"] ? bag["uri_with_datetime"].as<bool>() : true;

      static const auto package_path = ament_index_cpp::get_package_share_directory("lsy_ros_data_utils") + "/config/";
      br->spec.storage_config_uri = bag["storage_config_file"]
                                      ? package_path + bag["storage_config_file"].as<std::string>()
                                      : std::string("");

      compress_images_ = bag["compress_image"] ? bag["compress_image"].as<bool>() : false;

      jpeg_quality_ = bag["jpeg_quality"] ? bag["jpeg_quality"].as<int>() : 95;
      png_level_ = bag["png_level"] ? bag["png_level"].as<int>() : 3;
      compression_type_ = bag["compression_type"] ? bag["compression_type"].as<std::string>() : std::string("png");

      if (!bag["topics"] || !bag["topics"].IsSequence()) {
        throw std::runtime_error("Each bag must contain 'topics' as a sequence.");
      }

      for (const auto &t: bag["topics"]) {
        TopicSpec ts;
        ts.name = require_string(t, "name");
        ts.type = require_string(t, "type");
        ts.qos = t["qos"] ? t["qos"].as<std::string>() : std::string("default");
        ts.mode = t["mode"] ? t["mode"].as<std::string>() : std::string("auto");
        if (t["monitor"]) {
          ts.monitor_enabled = true;
          const auto mon = t["monitor"];
          ts.expected_hz = mon["expected_hz"] ? mon["expected_hz"].as<double>() : 0.0;
          ts.warn_below_hz = mon["warn_below_hz"] ? mon["warn_below_hz"].as<double>() : 0.0;
        }
        if (t["callback_group_id"] && t["callback_group_id"].as<size_t>() <= (cb_groups_.size() - 1)) {
          ts.cb_group = cb_groups_[t["callback_group_id"].as<size_t>()];
        } else {
          throw std::runtime_error("Each topic should be assigned to a valid callback group id. Topic: " + ts.name);
        }
        br->spec.topics.push_back(std::move(ts));
      }

      bags_.push_back(std::move(br));
    }

    // built_routing_tables
    topic_to_bags_.clear();
    topic_to_qos_.clear();
    topic_to_type_.clear();
    topic_to_mode_.clear();
    topic_stats_.clear();

    auto merged_qos = [](const std::string &a, const std::string &b) {
      // Policy: if ANY bag wants sensor_data => subscription uses sensor_data
      if (a == "sensor_data" || b == "sensor_data") return std::string("sensor_data");
      return std::string("default");
    };

    auto merged_mode = [](const std::string &a, const std::string &b) {
      // If user explicitly sets either typed or generic anywhere, prefer explicit over auto.
      // If conflicting explicit modes appear for same topic, we error later.
      if (a == "auto") return b;
      if (b == "auto") return a;
      return a; // keep (conflict checked separately)
    };

    for (size_t i = 0; i < bags_.size(); i++) {
      for (const auto &bag = bags_[i]->spec; const auto &ts: bag.topics) {
        topic_to_bags_[ts.name].push_back(i);

        // type consistency
        if (auto it = topic_to_type_.find(ts.name); it == topic_to_type_.end()) {
          topic_to_type_[ts.name] = ts.type;
        } else if (it->second != ts.type) {
          throw std::runtime_error(
            "Topic '" + ts.name + "' has conflicting types: '" + it->second + "' vs '" + ts.type + "'");
        }

        // qos merge
        if (auto qit = topic_to_qos_.find(ts.name); qit == topic_to_qos_.end()) {
          topic_to_qos_[ts.name] = ts.qos;
        } else {
          qit->second = merged_qos(qit->second, ts.qos);
        }

        // mode merge (single subscription)
        if (auto mit = topic_to_mode_.find(ts.name); mit == topic_to_mode_.end()) {
          topic_to_mode_[ts.name] = ts.mode;
        } else {
          const auto prev = mit->second;
          const auto now = merged_mode(prev, ts.mode);
          // conflict check: typed vs generic explicit
          if ((prev == "typed" && ts.mode == "generic") || (prev == "generic" && ts.mode == "typed")) {
            throw std::runtime_error(
              "Topic '" + ts.name + "' has conflicting modes across bags: 'typed' vs 'generic'");
          }
          mit->second = now;
        }

        // callback groups merge
        if (auto git = topic_to_callback_groups_.find(ts.name); git == topic_to_callback_groups_.end()) {
          topic_to_callback_groups_[ts.name] = ts.cb_group;
        } else if (git->second != ts.cb_group) {
          RCLCPP_WARN(get_logger(), " Overriding the callback group for the topic %s", ts.name.c_str());
        }

        // monitoring merge
        if (ts.monitor_enabled) {
          if (auto &st = topic_stats_[ts.name]; !st.enabled) {
            st.enabled = true;
            st.expected_hz = ts.expected_hz;
            st.warn_below_hz = ts.warn_below_hz;
          } else {
            // if repeated, require consistent thresholds (fail fast)
            if ((st.expected_hz != ts.expected_hz) || (st.warn_below_hz != ts.warn_below_hz)) {
              throw std::runtime_error("Monitoring config conflict for topic '" + ts.name + "'");
            }
          }
        } else {
          // ensure key exists only if enabled somewhere; otherwise absent (saves map lookups in callback)
        }
      }
    }
  }

  // ---------------- rosbag2 open & topics ----------------

  void
  BagRecorderComponent::open_bag_and_create_topics(BagRuntime &br) const {
    rosbag2_storage::StorageOptions storage_options;

    storage_options.uri = br.spec.bag_uri + "/" + br.spec.name;

    if (br.spec.uri_with_datetime) {
      storage_options.uri += "_";
      storage_options.uri += now_string_local();
    }

    storage_options.storage_id = br.spec.storage_id;
    storage_options.snapshot_mode = false;
    storage_options.max_cache_size = br.spec.bag_cache_size;
    storage_options.max_bagfile_size = br.spec.max_bag_size;

    rosbag2_cpp::ConverterOptions converter_options;
    converter_options.input_serialization_format = "cdr";
    converter_options.output_serialization_format = "cdr";

    // Apply the config URI if it was provided
    if (!br.spec.storage_config_uri.empty()) {
      storage_options.storage_config_uri = br.spec.storage_config_uri;
    }

    br.writer.open(storage_options, converter_options);

    // crate only this bag's topics
    for (const auto &ts: br.spec.topics) {
      rosbag2_storage::TopicMetadata md;
      md.name = ts.name;
      if (ts.type == "sensor_msgs/msg/Image" && compress_images_) {
        // override the message type if we want to compresse the images
        md.type = "sensor_msgs/msg/CompressedImage";
      } else {
        md.type = ts.type;
      }
      md.serialization_format = "cdr";
      br.writer.create_topic(md);
    }

    RCLCPP_INFO(get_logger(),
                "Opened bag '%s' uri='%s' storage_id='%s' topics=%zu max_queue_bytes=%zu overflow_policy=%s",
                br.spec.name.c_str(),
                storage_options.uri.c_str(),
                br.spec.storage_id.c_str(),
                br.spec.topics.size(),
                br.spec.max_queue_bytes,
                br.spec.overflow_policy == BagSpec::OverflowPolicy::BLOCK ? "BLOCK" : "DROP_OLDEST");
  }

  // ---------------- type registry (typed, intra-process compatible) ----------------
  template<typename MsgT>
  static std::function<void(const void *, rclcpp::SerializedMessage &)> make_serializer_fn() {
    return [](const void *msg_ptr, rclcpp::SerializedMessage &out) {
      const auto *m = static_cast<const MsgT *>(msg_ptr);
      rclcpp::Serialization<MsgT> ser;
      ser.serialize_message(m, &out);
    };
  }

  std::shared_ptr<rcutils_uint8_array_t>
  BagRecorderComponent::make_rcutils_uint8_array_copy(const rclcpp::SerializedMessage &serialized) {
    // We need rcutils_uint8_array_fini() on destruction.
    auto deleter = [](rcutils_uint8_array_t *arr) {
      if (!arr) return;
      rcutils_uint8_array_fini(arr);
      delete arr;
    };

    auto out = std::shared_ptr<rcutils_uint8_array_t>(new rcutils_uint8_array_t, deleter);
    *out = rcutils_get_zero_initialized_uint8_array();

    const auto &rcl = serialized.get_rcl_serialized_message();
    const size_t n = rcl.buffer_length;

    const rcutils_allocator_t allocator = rcl_get_default_allocator();
    if (RCUTILS_RET_OK != rcutils_uint8_array_init(out.get(), n, &allocator)) {
      throw std::runtime_error("Failed to initialize uint8_array of rcutils_uint8_array_init");
    }

    std::memcpy(out->buffer, rcl.buffer, n);
    out->buffer_length = n;
    return out;
  }

  std::shared_ptr<rcutils_uint8_array_t>
  BagRecorderComponent::make_rcutils_uint8_array_copy(const rcl_serialized_message_t &rcl) {
    auto deleter = [](rcutils_uint8_array_t *arr) {
      if (!arr) return;
      rcutils_uint8_array_fini(arr);
      delete arr;
    };

    auto out = std::shared_ptr<rcutils_uint8_array_t>(new rcutils_uint8_array_t, deleter);
    *out = rcutils_get_zero_initialized_uint8_array();

    const size_t n = rcl.buffer_length;

    const rcutils_allocator_t allocator = rcl_get_default_allocator();
    if (RCUTILS_RET_OK != rcutils_uint8_array_init(out.get(), n, &allocator)) {
      throw std::runtime_error("Failed rcutils_uint8_array_init");
    }

    std::memcpy(out->buffer, rcl.buffer, n);
    out->buffer_length = n;
    return out;
  }

  void BagRecorderComponent::fanout_enqueue_item(std::shared_ptr<rosbag2_storage::SerializedBagMessage> msg) {
    // Extract topic and calculate bytes directly from the serilaized payload
    const std::string &topic_name = msg->topic_name;
    const size_t msg_bytes = msg->serialized_data ? msg->serialized_data->buffer_length : 0;

    const auto it = topic_to_bags_.find(topic_name);
    if (it == topic_to_bags_.end()) return;

    for (const size_t bi: it->second) {
      auto &br = *bags_[bi];
      std::unique_lock<std::mutex> lk(br.mtx);

      // ---------- DROP_OLDEST MODE (default) ----------
      auto drop_oldest_one = [&]() {
        if (br.msg_queue.empty()) return;

        const auto &old = br.msg_queue.front();
        const size_t old_bytes = old->serialized_data ? old->serialized_data->buffer_length : 0;

        br.queued_bytes -= old_bytes;
        br.msg_queue.pop_front();

        br.dropped_msgs.fetch_add(1, std::memory_order_relaxed);
        br.dropped_bytes.fetch_add(old_bytes, std::memory_order_relaxed);
        br.overflow_events.fetch_add(1, std::memory_order_relaxed);
      };

      // ---------- BLOCK MODE ----------
      if (br.spec.overflow_policy == BagSpec::OverflowPolicy::BLOCK) {
        const bool would_block = (br.msg_queue.size() >= br.spec.max_queue_size) ||
                                 (br.queued_bytes + msg_bytes > br.spec.max_queue_bytes);
        if (would_block && br.spec.warn_on_overflow) {
          const int64_t now_ns = now_ns_steady();
          if (should_warn(br.last_overflow_warn_ns, now_ns, br.spec.overflow_warn_period_sec)) {
            RCLCPP_WARN(this->get_logger(),
                        "[bag=%s] OVERFLOW(block): writer is behind; blocking producer. queued=%zu msgs (%zu bytes), incoming=%zu bytes, limits: max_queue_size=%zu max_queue_bytes=%zu",
                        br.spec.name.c_str(),
                        br.msg_queue.size(), br.queued_bytes, msg_bytes,
                        br.spec.max_queue_size, br.spec.max_queue_bytes);
          }
        }

        br.cv.wait(lk, [&]() {
          if (br.stop.load()) return true;
          const bool under_count = (br.msg_queue.size() < br.spec.max_queue_size);
          const bool under_bytes = (br.queued_bytes + msg_bytes <= br.spec.max_queue_bytes);
          return under_count && under_bytes;
        });
        if (br.stop.load()) return;
        // enqueue
        br.msg_queue.push_back(msg);
        br.total_enqueued_msgs.fetch_add(1, std::memory_order_relaxed);
        br.queued_bytes += msg_bytes;

        lk.unlock();
        br.cv.notify_one();
        continue;
      }

      // ---------- DROP_OLDEST MODE (default) ----------
      // enforce count cap
      while (br.msg_queue.size() >= br.spec.max_queue_size && !br.msg_queue.empty()) {
        drop_oldest_one();
      }

      // enforce byte cap
      while (!br.msg_queue.empty() &&
             (br.queued_bytes + msg_bytes > br.spec.max_queue_bytes)) {
        drop_oldest_one();
      }

      // If a single message is larger than the entire cap, skip it (can't ever fit)
      if (msg_bytes > br.spec.max_queue_bytes) {
        if (br.spec.warn_on_overflow) {
          const int64_t now_ns = now_ns_steady();
          if (should_warn(br.last_overflow_warn_ns, now_ns, br.spec.overflow_warn_period_sec)) {
            RCLCPP_WARN(this->get_logger(),
                        "[bag=%s] OVERFLOW(drop_oldest): single message too large to fit cap; dropping incoming. incoming=%zu bytes cap=%zu bytes topic=%s",
                        br.spec.name.c_str(),
                        msg_bytes, br.spec.max_queue_bytes, topic_name.c_str());
          }
        }
        continue;
      }

      // If still can't fit (should be rare), drop incoming
      if (br.queued_bytes + msg_bytes > br.spec.max_queue_bytes) {
        br.dropped_msgs.fetch_add(1, std::memory_order_relaxed);
        br.dropped_bytes.fetch_add(msg_bytes, std::memory_order_relaxed);
        br.overflow_events.fetch_add(1, std::memory_order_relaxed);
      } else {
        br.msg_queue.push_back(msg);
        br.queued_bytes += msg_bytes;
        br.total_enqueued_msgs.fetch_add(1, std::memory_order_relaxed);
      }

      // Rate-limited aggregated warning
      if (br.spec.warn_on_overflow) {
        const auto current_dm = br.dropped_msgs.load(std::memory_order_relaxed);
        if (current_dm > 0) {
          // Only check/consume the timer if we actually dropped something
          const int64_t now_ns = now_ns_steady();

          if (should_warn(br.last_overflow_warn_ns, now_ns, br.spec.overflow_warn_period_sec)) {
            // Safely extract the counts and reset to 0
            const auto dm = br.dropped_msgs.exchange(0, std::memory_order_relaxed);
            const auto db = br.dropped_bytes.exchange(0, std::memory_order_relaxed);

            RCLCPP_WARN(this->get_logger(),
                        "[bag=%s] OVERFLOW(drop_oldest): dropped=%llu msgs (%llu bytes) | queued=%zu msgs (%zu bytes) | limits: max_queue_size=%zu max_queue_bytes=%zu",
                        br.spec.name.c_str(),
                        static_cast<unsigned long long>(dm),
                        static_cast<unsigned long long>(db),
                        br.msg_queue.size(), br.queued_bytes,
                        br.spec.max_queue_size, br.spec.max_queue_bytes);
          }
        }
      }
      // Don't forget to unlock and notify the writer thread!
      lk.unlock();
      br.cv.notify_one();
    }
  }

  // ---------------- writer thread ----------------

  void
  BagRecorderComponent::writer_loop(BagRuntime *br) const {
    while (!br->stop.load() && rclcpp::ok()) {
      std::deque<std::shared_ptr<rosbag2_storage::SerializedBagMessage> > batch; {
        std::unique_lock<std::mutex> lk(br->mtx);
        br->cv.wait(lk, [&]() { return br->stop.load() || !br->msg_queue.empty(); });

        // Ensure we flush any remaining messages before exiting on shutdown
        if (br->stop.load() && br->msg_queue.empty()) break;

        // O(1) swap: Instantly transfers all pointers to the local batch
        batch.swap(br->msg_queue);
        br->queued_bytes = 0;
      }

      // Pure disk I/O execution. Zero heap allocations happening here!
      for (const auto &bag_msg: batch) {
        br->writer.write(bag_msg);
        br->total_written_msgs.fetch_add(1, std::memory_order_relaxed);
      }
      // If producers are blocked, wake them
      br->cv.notify_all();
    }

    // drain on shutdown
    std::deque<std::shared_ptr<rosbag2_storage::SerializedBagMessage> > batch_tail; {
      std::lock_guard<std::mutex> lk(br->mtx);
      batch_tail.swap(br->msg_queue);
      br->queued_bytes = 0;
    }
    for (const auto &bag_msg: batch_tail) {
      br->writer.write(bag_msg);
      br->total_written_msgs.fetch_add(1, std::memory_order_relaxed);
    }
    // --- LOOP EXITED. TIME TO SHUT DOWN ---
    RCLCPP_INFO(get_logger(), "Draining complete. Closing bag '%s'...", br->spec.name.c_str());
    br->writer.close();
    RCLCPP_INFO(get_logger(), "Writer thread stopped for bag '%s'", br->spec.name.c_str());
  }

  void
  BagRecorderComponent::create_subscriptions() {
    subs_.clear();
    subs_.reserve(topic_to_type_.size());

    for (const auto &[topic, type]: topic_to_type_) {
      if (auto it = type_registry_.find(type); it == type_registry_.end()) {
        RCLCPP_ERROR(get_logger(),
                     "Type not registered (cannot create typed intra-process subscription): type='%s' topic='%s'",
                     type.c_str(), topic.c_str());
        continue;
      }
      const auto qos = qos_from_string(topic_to_qos_.at(topic));
      const auto mode = topic_to_mode_.contains(topic) ? topic_to_mode_.at(topic) : std::string("auto");

      const bool typed_available = (type_registry_.contains(type));
      const bool want_typed = (mode == "typed") || (mode == "auto" && typed_available);
      const bool want_generic = (mode == "generic") || (mode == "auto" && !typed_available);

      if (want_typed) {
        // if typed, we assume that the message was passed through the intra comm process
        auto sub = type_registry_.at(type)(topic, qos);
        subs_.push_back(sub);
        continue;
      }

      if (want_generic) {
        // Generic subscription: callback receives serialized bytes already.
        auto cb = [this, topic](std::shared_ptr<rclcpp::SerializedMessage> msg) {
          if (this->is_shutting_down_.load(std::memory_order_relaxed)) return;

          //this->active_callbacks_.fetch_add(1, std::memory_order_acquire);
          //   struct CallbackTracker {
          //     std::atomic<int>& count;
          //     CallbackTracker(std::atomic<int>& c) : count(c) {}
          //     ~CallbackTracker() {
          //       count.fetch_sub(1, std::memory_order_release);
          //     }
          //    } tracker(this->active_callbacks_);

          // monitoring
          const int64_t recv_ns_steady = now_ns_steady();
          this->on_rx(topic, recv_ns_steady);

          const int64_t recv_ns = this->now().nanoseconds();
          const int64_t send_ns = recv_ns; // can't extract header stamp without deserializing

          const auto &rin = msg->get_rcl_serialized_message();
          auto payload = BagRecorderComponent::make_rcutils_uint8_array_copy(rin);

          // Use the new helper!
          this->build_and_enqueue_msg(topic, send_ns, recv_ns, std::move(payload));
        };

        auto subscription_options = rclcpp::SubscriptionOptions();
        subscription_options.callback_group = topic_to_callback_groups_.at(topic);

        auto sub = this->create_generic_subscription(
          topic,
          type,
          qos,
          std::move(cb),
          subscription_options);

        subs_.push_back(sub);
        continue;
      }

      // Should never reach here
      RCLCPP_ERROR(get_logger(), "Failed to create subscription for topic='%s' type='%s'", topic.c_str(), type.c_str());
    }
  }

  void BagRecorderComponent::build_and_enqueue_msg(const std::string &topic_name, int64_t send_ns, int64_t recv_ns,
                                                   std::shared_ptr<rcutils_uint8_array_t> payload) {
    auto bag_msg = std::make_shared<rosbag2_storage::SerializedBagMessage>();
    bag_msg->topic_name = topic_name;
    this->set_bag_message_timestamps(*bag_msg, send_ns, recv_ns);
    bag_msg->serialized_data = std::move(payload);

    this->fanout_enqueue_item(std::move(bag_msg));
  }

  //----------------- monitoring --------------------
  void BagRecorderComponent::on_rx(const std::string &topic, int64_t recv_ns_steady) {
    if (!monitor_cfg_.enabled) return;

    const auto it = topic_stats_.find(topic);
    if (it == topic_stats_.end()) return;

    TopicStats &st = it->second;
    if (!st.enabled) return;

    st.count.fetch_add(1, std::memory_order_relaxed);

    int64_t expected0 = 0;
    (void) st.first_rx_ns.compare_exchange_strong(expected0, recv_ns_steady, std::memory_order_relaxed);

    const int64_t prev = st.last_rx_ns.exchange(recv_ns_steady, std::memory_order_relaxed);
    if (prev == 0) return;

    const double dt = (recv_ns_steady - prev) * 1e-9;
    if (dt <= 0.0) return;

    const double inst_hz = 1.0 / dt;

    const double tau = monitor_cfg_.ewma_tau_sec;
    const double alpha = (tau > 1e-6) ? (1.0 - std::exp(-dt / tau)) : 1.0;

    const double prev_ewma = st.ewma_hz.load(std::memory_order_relaxed);
    const double next_ewma = (prev_ewma <= 0.0) ? inst_hz : (prev_ewma + alpha * (inst_hz - prev_ewma));
    st.ewma_hz.store(next_ewma, std::memory_order_relaxed);

    if (st.warn_below_hz > 0.0) {
      if (next_ewma < st.warn_below_hz) {
        int64_t zero = 0;
        (void) st.below_since_ns.compare_exchange_strong(zero, recv_ns_steady, std::memory_order_relaxed);
      } else {
        st.below_since_ns.store(0, std::memory_order_relaxed);
      }
    }
  }

  void BagRecorderComponent::monitor_timer_cb() {
    if (!monitor_cfg_.enabled) return;
    static bool first_run = true;

    // Clear terminal + move cursor to top-left
    std::cout << "\033[2J\033[H" << std::flush;
    const int64_t now_ns = now_ns_steady();

    // ==========================================
    // 1. TOPIC STATISTICS (Received)
    // ==========================================

    RCLCPP_INFO(get_logger(), "===== BagRecorder Monitor (EWMA tau=%.2fs) =====", monitor_cfg_.ewma_tau_sec);
    RCLCPP_INFO(get_logger(), "%-40s  %8s  %8s  %8s  %s",
                "topic", "ewmaHz", "warn<", "count", "rate");

    static std::vector<std::pair<std::string, TopicStats *> > topic_stats_container;

    if (first_run) {
      topic_stats_container.reserve(topic_stats_.size());

      for (auto &kv: topic_stats_) {
        topic_stats_container.emplace_back(kv.first, &kv.second);
      }

      // Sort by topic name
      std::sort(topic_stats_container.begin(), topic_stats_container.end(),
                [](const auto &a, const auto &b) {
                  return a.first < b.first;
                });
      first_run = false;
    }

    for (auto &kv: topic_stats_container) {
      const std::string &topic = kv.first;
      TopicStats &st = *kv.second;
      if (!st.enabled) continue;

      const double ewma = st.ewma_hz.load(std::memory_order_relaxed);
      const uint64_t cnt = st.count.load(std::memory_order_relaxed);
      const double expected = st.expected_hz;
      const double warn_below = st.warn_below_hz;

      bool warn = false;
      if (warn_below > 0.0) {
        if (const int64_t below_since = st.below_since_ns.load(std::memory_order_relaxed); below_since != 0) {
          if (const double dur = (now_ns - below_since) * 1e-9; dur >= monitor_cfg_.warn_grace_sec) warn = true;
        }
      }

      const double ref = (expected > 0.0) ? expected : ((warn_below > 0.0) ? warn_below : 1.0);
      const std::string b = bar(ewma, ref, 20);

      char line[256];
      std::snprintf(line, sizeof(line),
                    "%-40s  %8.2f  %8.2f  %8llu  [%s]",
                    topic.c_str(), ewma, warn_below,
                    static_cast<unsigned long long>(cnt), b.c_str());

      if (warn) {
        RCLCPP_WARN(get_logger(), "%s", maybe_red(line).c_str());
      } else if (warn_below > 0.0 && ewma > 0.0 && ewma < warn_below) {
        RCLCPP_INFO(get_logger(), "%s", maybe_yellow(line).c_str());
      } else {
        RCLCPP_INFO(get_logger(), "%s", line);
      }
    }

    // ==========================================
    // 2. BAG I/O STATISTICS (Written vs Dropped)
    // ==========================================
    RCLCPP_INFO(get_logger(), " ");
    RCLCPP_INFO(get_logger(), "===== Bag I/O Statistics =====");
    RCLCPP_INFO(get_logger(), "%-20s  %10s  %10s  %10s  %10s",
                "Bag Name", "Enqueued", "Written", "Dropped", "In Queue");

    for (const auto &br: bags_) {
      const auto enqueued = br->total_enqueued_msgs.load(std::memory_order_relaxed);
      const auto written = br->total_written_msgs.load(std::memory_order_relaxed);
      const auto dropped = br->dropped_msgs.load(std::memory_order_relaxed);

      // Lock-free queue size estimation
      const int64_t in_queue = enqueued - written - dropped;

      char bag_line[256];
      std::snprintf(bag_line, sizeof(bag_line),
                    "%-20s  %10llu  %10llu  %10llu  %10lld",
                    br->spec.name.c_str(),
                    static_cast<unsigned long long>(enqueued),
                    static_cast<unsigned long long>(written),
                    static_cast<unsigned long long>(dropped),
                    static_cast<long long>(in_queue > 0 ? in_queue : 0));

      if (dropped > 0) {
        RCLCPP_WARN(get_logger(), "%s", maybe_red(bag_line).c_str());
      } else {
        RCLCPP_INFO(get_logger(), "%s", bag_line);
      }
    }
  }
}

#include <rclcpp_components/register_node_macro.hpp>
RCLCPP_COMPONENTS_REGISTER_NODE(lsy_ros_data_utils::rosbag::BagRecorderComponent)

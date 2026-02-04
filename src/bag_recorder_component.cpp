// Copyright (c) 2026  Learning Systems and Robotics Lab,
// Technical University of Munich (TUM)
//
// Authors:  Haoming Zhang <haoming.zhang@tum>
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

// Prefer new API (send_timestamp/recv_timestamp) if it exists
template<typename M>
auto set_ts_impl(M &m, int64_t send_ns, int64_t recv_ns, int)
  -> decltype((m.send_timestamp = send_ns, m.recv_timestamp = recv_ns), void()) {
  m.send_timestamp = send_ns;
  m.recv_timestamp = recv_ns;
}

// Fallback to old API (time_stamp) if it exists
template<typename M>
auto set_ts_impl(M &m, int64_t send_ns, int64_t /*recv_ns*/, long)
  -> decltype((m.time_stamp = send_ns), void()) {
  m.time_stamp = send_ns;
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

  // ---------------- main ----------------

  BagRecorderComponent::BagRecorderComponent(const rclcpp::NodeOptions &node_options)
    : rclcpp::Node("bag_recorder_component", ensureIntraProcesComm(node_options)) {
    this->declare_parameter<std::string>("config_file", "");
    const auto cfg = this->get_parameter("config_file").as_string();
    if (cfg.empty()) {
      throw std::runtime_error("Parameter 'config_file' is required.");
    }
    this->init_type_registry();
    this->load_config(cfg);

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
    for (const auto &br: bags_) {
      br->stop.store(true);
      br->cv.notify_all();
    }
    for (const auto &br: bags_) {
      if (br->thread.joinable()) {
        br->thread.join();
      }
    }
    RCLCPP_INFO(get_logger(), "BagRecorderComponent destroyed.");
  }

  void
  BagRecorderComponent::load_config(const std::string &yaml_path) {
    YAML::Node config = YAML::LoadFile(yaml_path);
    if (!config["bags"] || !config["bags"].IsSequence()) {
      throw std::runtime_error("YAML must contain 'bags' as a sequence.");
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
      br->spec.write_frequency_hz = bag["write_frequency_hz"] ? bag["write_frequency_hz"].as<double>() : 1.0;
      br->spec.max_queue_size = bag["max_queue_size"] ? bag["max_queue_size"].as<size_t>() : 20000;
      br->spec.uri_with_datetime = bag["uri_with_datetime"] ? bag["uri_with_datetime"].as<bool>() : true;

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
        br->spec.topics.push_back(std::move(ts));
      }

      if (br->spec.write_frequency_hz <= 0.0) {
        throw std::runtime_error("write_frequency_hz must be > 0 for bag " + br->spec.name);
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

    rosbag2_cpp::ConverterOptions converter_options;
    converter_options.input_serialization_format = "cdr";
    converter_options.output_serialization_format = "cdr";

    br.writer.open(storage_options, converter_options);

    // crate only this bag's topics
    for (const auto &ts: br.spec.topics) {
      rosbag2_storage::TopicMetadata md;
      md.name = ts.name;
      md.type = ts.type;
      md.serialization_format = "cdr";
      br.writer.create_topic(md);
    }

    RCLCPP_INFO(get_logger(),
                "Opened bag '%s' uri='%s' storage_id='%s' topics=%zu",
                br.spec.name.c_str(), br.spec.bag_uri.c_str(), br.spec.storage_id.c_str(), br.spec.topics.size());
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
  
  void
  BagRecorderComponent::fanout_enqueue_item(const std::string &topic_name,
                                            std::shared_ptr<const BagItem> item) {
    const auto it = topic_to_bags_.find(topic_name);
    if (it == topic_to_bags_.end()) {
      return;
    }

    for (const size_t bi: it->second) {
      auto &br = *bags_[bi]; {
        std::lock_guard<std::mutex> lk(br.mtx);
        if (br.msg_queue.size() >= br.spec.max_queue_size) {
          br.msg_queue.pop_front();
        }
        br.msg_queue.push_back(std::move(item));
      }
      br.cv.notify_one();
    }
  }

  // ---------------- writer thread ----------------

  void
  BagRecorderComponent::writer_loop(BagRuntime *br) const {
    using namespace std::chrono;
    const auto period = duration<double>(1.0 / br->spec.write_frequency_hz);
    auto next_wake = steady_clock::now();

    auto prepare_rosbag_serialized_msg = [this](
      const std::shared_ptr<const BagItem> &item) -> std::shared_ptr<
      rosbag2_storage::SerializedBagMessage> {
      // for typed messages that have not been serialized due to the intra comm process, we call this function
      // Serialize raw message (CDR)
      const auto serialized = item->make_serialized();

      // Copy into rcutils buffer expected by rosbag2
      const auto payload = make_rcutils_uint8_array_copy(serialized);

      // Construct bag message
      auto bag_msg = std::make_shared<rosbag2_storage::SerializedBagMessage>();
      bag_msg->topic_name = item->topic_name;
      bag_msg->serialized_data = payload;

      // Cross-distro timestamp handling
      set_bag_message_timestamps(*bag_msg, item->send_ns, item->recv_ns);

      return bag_msg;
    };

    while (!br->stop.load() && rclcpp::ok()) {
      //RCLCPP_INFO(get_logger(), "Writer thread writting for bag '%s'", br->spec.name.c_str());
      next_wake += duration_cast<steady_clock::duration>(period);

      std::deque<std::shared_ptr<const BagItem> > batch; {
        std::unique_lock<std::mutex> lk(br->mtx);
        br->cv.wait_until(lk, next_wake, [&]() { return br->stop.load() || !br->msg_queue.empty(); });
        if (br->stop.load()) break;
        batch.swap(br->msg_queue);
      }

      for (const auto &item: batch) {
        br->writer.write(prepare_rosbag_serialized_msg(item));
      }
      std::this_thread::sleep_until(next_wake);
    }

    // drain on shutdown
    std::deque<std::shared_ptr<const BagItem> > batch_tail; {
      std::lock_guard<std::mutex> lk(br->mtx);
      batch_tail.swap(br->msg_queue);
    }
    for (const auto &item: batch_tail) {
      br->writer.write(prepare_rosbag_serialized_msg(item));
    }
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
          // monitoring
          const int64_t recv_ns_steady = now_ns_steady();
          this->on_rx(topic, recv_ns_steady);

          const int64_t recv_ns = this->now().nanoseconds();
          const int64_t send_ns = recv_ns; // can't extract header stamp without deserializing

          auto item = std::make_shared<BagItem>();
          item->topic_name = topic;
          item->send_ns = send_ns;
          item->recv_ns = recv_ns;

          // Capture the serialized message and copy it into 'out' later.
          item->make_serialized = [msg]() -> rclcpp::SerializedMessage {
            // Copy construct a new SerializedMessage with the right size and memcpy the bytes
            const auto &rin = msg->get_rcl_serialized_message();
            const size_t n = rin.buffer_length;

            rclcpp::SerializedMessage out(n);
            auto &rout = out.get_rcl_serialized_message();
            std::memcpy(rout.buffer, rin.buffer, n);
            rout.buffer_length = n;

            return out;
          };

          this->fanout_enqueue_item(topic, std::move(item));
        };

        auto sub = this->create_generic_subscription(
          topic,
          type,
          qos,
          std::move(cb));

        subs_.push_back(sub);
        continue;
      }

      // Should never reach here
      RCLCPP_ERROR(get_logger(), "Failed to create subscription for topic='%s' type='%s'", topic.c_str(), type.c_str());
    }
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

     // Clear terminal + move cursor to top-left
    std::cout << "\033[2J\033[H" << std::flush;
    const int64_t now_ns = now_ns_steady();

    RCLCPP_INFO(get_logger(), "===== BagRecorder Monitor (EWMA tau=%.2fs) =====", monitor_cfg_.ewma_tau_sec);
    RCLCPP_INFO(get_logger(), "%-40s  %8s  %8s  %8s  %s",
                "topic", "ewmaHz", "warn<", "count", "rate");

    for (auto &kv: topic_stats_) {
      const std::string &topic = kv.first;
      TopicStats &st = kv.second;
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
  }
}

#include <rclcpp_components/register_node_macro.hpp>
RCLCPP_COMPONENTS_REGISTER_NODE(lsy_ros_data_utils::rosbag::BagRecorderComponent)

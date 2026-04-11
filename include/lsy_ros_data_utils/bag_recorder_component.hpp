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

#ifndef LSY_ROS_DATA_UTILS_BAG_RECORDER_COMPONENT_HPP
#define LSY_ROS_DATA_UTILS_BAG_RECORDER_COMPONENT_HPP
// general utils
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <type_traits>

// ros utils
#include <rclcpp/rclcpp.hpp>
#include <rclcpp_components/component_manager.hpp>
#include <rosbag2_cpp/writer.hpp>
#include <rosbag2_storage/serialized_bag_message.hpp>
#include <rclcpp/serialization.hpp>
#include <rosbag2_storage/topic_metadata.hpp>

#include <sensor_msgs/msg/image.hpp>
#include <sensor_msgs/msg/compressed_image.hpp>

#include <opencv2/imgcodecs.hpp>
#include <opencv2/imgproc.hpp>

#include "utils.hpp"
#include "msg_converter.hpp"

namespace lsy_ros_data_utils::rosbag {
  struct TopicSpec {
    std::string name;
    std::string type;
    std::string qos; // "sensor_data" or "default"
    std::string mode; // "auto" or "typed" for intra communication process or "generic" with serialization
    rclcpp::CallbackGroup::SharedPtr cb_group;
    bool monitor_enabled{false};
    double expected_hz{0.0};
    double warn_below_hz{0.0};
  };

  struct BagSpec {
    enum class OverflowPolicy {
      DROP_OLDEST,
      BLOCK
    };

    std::string name;
    std::string bag_uri;
    bool uri_with_datetime{false};
    bool enable_message_monitoring{false};
    std::string storage_id{"mcap"};
    std::string storage_config_uri{""}; // <--- Add this field

    // queue limits
    size_t max_queue_size{20000}; // secondary cap by count
    size_t max_queue_bytes{1024ull * 1024 * 1024}; // 1 GiB per bag default
    size_t bag_cache_size{2ull * 1024 * 1024 * 1024}; // 2 GiB per bag default

    // overflow policy: "drop_oldest" (default) or "block"
    OverflowPolicy overflow_policy = OverflowPolicy::DROP_OLDEST;
    std::vector<TopicSpec> topics;

    bool warn_on_overflow{true};
    double overflow_warn_period_sec{1.0}; // rate-limit warnings
  };

  class BagRecorderComponent final : public rclcpp::Node {
  public:
    explicit BagRecorderComponent(const rclcpp::NodeOptions &node_options);

    ~BagRecorderComponent() override;

  private:
    // ---------- config ----------
    void load_config(const std::string &yaml_path);

    // ---------- subscriptions ----------
    void create_subscriptions();

    void build_and_enqueue_msg(
      const std::string &topic_name,
      int64_t send_ns,
      int64_t recv_ns,
      std::shared_ptr<rcutils_uint8_array_t> payload);

    struct MonitorCfg {
      bool enabled{false};
      double print_hz{1.0};
      double ewma_tau_sec{2.0};
      double warn_grace_sec{2.0};
      bool use_ansi_color{true};
    };

    struct TopicStats {
      bool enabled{false};
      double expected_hz{0.0};
      double warn_below_hz{0.0};

      std::atomic<uint64_t> count{0};
      std::atomic<int64_t> first_rx_ns{0}; // steady clock ns
      std::atomic<int64_t> last_rx_ns{0}; // steady clock ns
      std::atomic<double> ewma_hz{0.0};
      std::atomic<int64_t> below_since_ns{0}; // steady clock ns, 0 => not below
    };

    // bag runtime
    struct BagRuntime {
      BagSpec spec;
      rosbag2_cpp::Writer writer;

      std::mutex mtx;
      std::condition_variable cv;

      //std::deque<std::shared_ptr<const BagItem> > msg_queue;
      std::deque<std::shared_ptr<rosbag2_storage::SerializedBagMessage> > msg_queue;
      size_t queued_bytes{0};

      std::atomic<bool> stop{false};
      std::thread thread;

      std::atomic<uint64_t> dropped_msgs{0};
      std::atomic<uint64_t> dropped_bytes{0};
      std::atomic<uint64_t> overflow_events{0};
      std::atomic<int64_t> last_overflow_warn_ns{0}; // steady clock ns
    };

    void open_bag_and_create_topics(BagRuntime &br) const;

    void writer_loop(BagRuntime *br) const;

    // message handling
    static std::shared_ptr<rcutils_uint8_array_t> make_rcutils_uint8_array_copy(
      const rclcpp::SerializedMessage &serialized);

    static std::shared_ptr<rcutils_uint8_array_t> make_rcutils_uint8_array_copy(
      const rcl_serialized_message_t &rcl);

    void set_bag_message_timestamps(
      rosbag2_storage::SerializedBagMessage &m,
      int64_t send_ns,
      int64_t recv_ns) const;

    void init_type_registry();

    template<typename MsgT>
    void register_type(const std::string &type_name); // implemented below (header)

    void fanout_enqueue_item(std::shared_ptr<rosbag2_storage::SerializedBagMessage> msg);

    // topic statics
    void on_rx(const std::string &topic, int64_t recv_ns_steady);

    void monitor_timer_cb();

  private: // variables
    // general parameters
    std::string cfg_file_;
    bool compress_images_{false};
    int jpeg_quality_{95};
    int png_level_{3};
    std::string compression_type_{"png"};

    std::atomic<bool> is_shutting_down_{false};
    std::atomic<int> active_callbacks_{0};
    // loaded bags
    std::vector<std::unique_ptr<BagRuntime> > bags_;

    // routing: topic -> which bags want it
    std::unordered_map<std::string, std::vector<size_t> > topic_to_bags_;
    // For creating exactly one subscription per topic
    std::unordered_map<std::string, std::string> topic_to_type_;
    std::unordered_map<std::string, std::string> topic_to_qos_; // merged qos policy
    std::unordered_map<std::string, std::string> topic_to_mode_; // per unique topic
    std::unordered_map<std::string, rclcpp::CallbackGroup::SharedPtr> topic_to_callback_groups_;

    std::vector<rclcpp::CallbackGroup::SharedPtr> cb_groups_;
    size_t next_cb_group_idx_{0};

    // registry: "sensor_msgs/msg/Image" -> factory that creates typed sub
    using SubFactory = std::function<rclcpp::SubscriptionBase::SharedPtr(
      const std::string &topic_name,
      const rclcpp::QoS &qos)>;

    std::unordered_map<std::string, SubFactory> type_registry_;

    // Keep subscriptions alive
    std::vector<rclcpp::SubscriptionBase::SharedPtr> subs_;

    // monitoring runtime
    MonitorCfg monitor_cfg_;
    rclcpp::TimerBase::SharedPtr monitor_timer_;
    std::unordered_map<std::string, TopicStats> topic_stats_;
  };

  // ============ template implementation (must be in header) ============

  template<typename MsgT>
  void BagRecorderComponent::register_type(const std::string &type_name) {
    // Store a factory that will create a typed subscription for MsgT.
    // This keeps intra-process compatibility because it is a typed rclcpp subscription.
    auto compress_images = false;
    if (type_name == "sensor_msgs/msg/Image" && compress_images_) {
      compress_images = true;
    }

    type_registry_[type_name] =
        [this, compress_images](const std::string &topic_name,
                                const rclcpp::QoS &qos) -> rclcpp::SubscriptionBase::SharedPtr {
          // serializer for MsgT (type-erased via void*)

          // callback receives a shared_ptr to the in-memory message (intra-process friendly)
          auto cb = [this, topic_name, compress_images](typename MsgT::ConstSharedPtr msg) {

            if (this->is_shutting_down_.load(std::memory_order_relaxed)) {
              return;
            }

            // 2. Track this active callback
            this->active_callbacks_.fetch_add(1, std::memory_order_acquire);
            struct CallbackTracker {
              std::atomic<int>& count;
              CallbackTracker(std::atomic<int>& c) : count(c) {}
              ~CallbackTracker() { 
                count.fetch_sub(1, std::memory_order_release); 
              }
            } tracker(this->active_callbacks_);

            
            // monitoring: steady clock, lock-free
            const int64_t recv_ns_steady = now_ns_steady();
            this->on_rx(topic_name, recv_ns_steady);

            const int64_t recv_ns = this->now().nanoseconds();
            const int64_t send_ns = get_message_timestamp_ns(*msg, recv_ns);

            std::string topic_name_bag = topic_name;

            // erase type without copying payload
            rclcpp::SerializedMessage serialized_msg;

            if constexpr (std::is_same_v<MsgT, sensor_msgs::msg::Image>) {
              if (compress_images) {
                topic_name_bag += "/compressed";
                const rclcpp::Serialization<sensor_msgs::msg::CompressedImage> ser;
                const auto compressed_img = toCompressed(msg, compression_type_, jpeg_quality_, png_level_);
                ser.serialize_message(&compressed_img, &serialized_msg);
              } else {
                // dirty fix
                rclcpp::Serialization<MsgT> ser;
                ser.serialize_message(msg.get(), &serialized_msg);
              }
            } else {
              rclcpp::Serialization<MsgT> ser;
              ser.serialize_message(msg.get(), &serialized_msg);
            }

            auto payload = std::shared_ptr<rcutils_uint8_array_t>(
              new rcutils_uint8_array_t(serialized_msg.release_rcl_serialized_message()),
              [](rcutils_uint8_array_t *arr) {
                if (arr) {
                  rcutils_uint8_array_fini(arr);
                  delete arr;
                }
              });

            // enqueue raw message pointer + serializer; writer thread will serialize
            this->build_and_enqueue_msg(topic_name_bag, send_ns, recv_ns, std::move(payload));
          };
          auto subscription_options = rclcpp::SubscriptionOptions();
          subscription_options.callback_group = topic_to_callback_groups_.at(topic_name);
          return this->create_subscription<MsgT>(topic_name, qos, std::move(cb), subscription_options);
        };
  }
}

#endif //LSY_ROS_DATA_UTILS_BAG_RECORDER_COMPONENT_HPP

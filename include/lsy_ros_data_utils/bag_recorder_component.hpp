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
    bool monitor_enabled{false};
    double expected_hz{0.0};
    double warn_below_hz{0.0};
  };

  struct BagSpec {
    std::string name;
    std::string bag_uri;
    bool uri_with_datetime{false};
    bool enable_message_monitoring{false};
    std::string storage_id{"mcap"};
    // (legacy) kept for compatibility; writer loop is drain-driven now
    double write_frequency_hz{1};

    // queue limits
    size_t max_queue_size{20000}; // secondary cap by count
    size_t max_queue_bytes{1024ull * 1024 * 1024}; // 1 GiB per bag default

    // overflow policy: "drop_oldest" (default) or "block"
    std::string overflow_policy{"drop_oldest"};
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

    // -------- Bag item (unifies typed+generic) --------
    struct BagItem {
      std::string topic_name;
      int64_t send_ns{0}; // publish time (header stamp if available else recv)
      int64_t recv_ns{0}; // receipt time
      // produces serialized bytes when called by writer thread
      //std::function<rclcpp::SerializedMessage()> make_serialized;

      // serialized bytes ready to write
      std::shared_ptr<rcutils_uint8_array_t> payload;
      size_t approx_bytes{0};
    };

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

      std::deque<std::shared_ptr<const BagItem> > msg_queue;
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

    void fanout_enqueue_item(
      const std::string &topic_name,
      std::shared_ptr<const BagItem> item);

    // topic statics
    void on_rx(const std::string &topic, int64_t recv_ns_steady);
    void monitor_timer_cb();

  private: // variables
    // general parameters
    bool compress_images_{false};
    int jpeg_quality_{95};
    int png_level_{3};
    std::string compression_type_{"png"};
    // loaded bags
    std::vector<std::unique_ptr<BagRuntime> > bags_;

    // routing: topic -> which bags want it
    std::unordered_map<std::string, std::vector<size_t> > topic_to_bags_;
    // For creating exactly one subscription per topic
    std::unordered_map<std::string, std::string> topic_to_type_;
    std::unordered_map<std::string, std::string> topic_to_qos_; // merged qos policy
    std::unordered_map<std::string, std::string> topic_to_mode_; // per unique topic

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
        [this, compress_images](const std::string &topic_name, const rclcpp::QoS &qos) -> rclcpp::SubscriptionBase::SharedPtr {
          // serializer for MsgT (type-erased via void*)

          // callback receives a shared_ptr to the in-memory message (intra-process friendly)
          auto cb = [this, topic_name, compress_images](typename MsgT::ConstSharedPtr msg) {
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
              }
            } else {
              rclcpp::Serialization<MsgT> ser;
              ser.serialize_message(msg.get(), &serialized_msg);
            }
            auto payload = BagRecorderComponent::make_rcutils_uint8_array_copy(serialized_msg);
            auto bag_item = std::make_shared<BagItem>();
            bag_item->topic_name = topic_name_bag;
            bag_item->send_ns = send_ns;
            bag_item->recv_ns = recv_ns;
            bag_item->payload = std::move(payload);
            bag_item->approx_bytes = bag_item->payload ? bag_item->payload->buffer_length : 0;

            // enqueue raw message pointer + serializer; writer thread will serialize
            this->fanout_enqueue_item(topic_name, std::move(bag_item));
          };

          return this->create_subscription<MsgT>(topic_name, qos, std::move(cb));
        };
  }
}

#endif //LSY_ROS_DATA_UTILS_BAG_RECORDER_COMPONENT_HPP

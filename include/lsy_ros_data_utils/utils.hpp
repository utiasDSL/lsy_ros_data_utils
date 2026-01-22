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

#ifndef LSY_ROS_DATA_UTILS_UTILS_HPP
#define LSY_ROS_DATA_UTILS_UTILS_HPP

#include <type_traits>
#include <utility>
#include <yaml-cpp/yaml.h>
#include <rclcpp/rclcpp.hpp>

#include <iomanip>
#include <sstream>
#include <ctime>

namespace lsy_ros_data_utils {
  // -------------- Output utils --------------

  inline std::string bar(const double value, double ref, const int width) {
    if (ref <= 0.0) ref = 1.0;
    double ratio = value / ref;
    if (ratio < 0.0) ratio = 0.0;
    if (ratio > 1.0) ratio = 1.0;

    const int filled = static_cast<int>(ratio * width + 0.5); // round nicely

    return std::string(static_cast<size_t>(filled), '#') +
           std::string(static_cast<size_t>(width - filled), '-');
  };

  inline std::string maybe_red(const std::string &s) {
    return std::string("\033[31m") + s + "\033[0m";
  };

  inline std::string maybe_yellow(const std::string &s) {
    return std::string("\033[33m") + s + "\033[0m";
  };
  
  // -------------- Memory utils --------------
  // Not all platforms provide lock-free atomic<double>. This is still very cheap.
  inline void atomic_store_double(std::atomic<double> &a, double v) {
    a.store(v, std::memory_order_relaxed);
  }
  
  // -------------- Date & Time utils --------------
  inline std::string now_string_local() {
    using clock = std::chrono::system_clock;
    const auto tp = clock::now();

    const auto t = clock::to_time_t(tp);
    std::tm tm{};
#ifdef _WIN32
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif

    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d_%H-%M-%S");

    // Optional: milliseconds
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()) % 1000;
    oss << "." << std::setfill('0') << std::setw(3) << ms.count();

    // Make it filesystem safe just in case (avoid ':' etc.)
    auto s = oss.str();
    for (char &c: s) {
      if (c == ':' || c == ' ') c = '_';
    }
    return s;
  }

  using SteadyClock = std::chrono::steady_clock;

  inline int64_t now_ns_steady() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
          SteadyClock::now().time_since_epoch())
        .count();
  }

  inline double ns_to_sec(int64_t ns) { return static_cast<double>(ns) * 1e-9; }

  inline double safe_div(double a, double b) { return (b > 0.0) ? (a / b) : 0.0; }

  inline double atomic_load_double(const std::atomic<double> &a) {
    return a.load(std::memory_order_relaxed);
  }

  // ------------------  ROS utils  ---------------------

  inline rclcpp::NodeOptions ensureIntraProcesComm(const rclcpp::NodeOptions &options) {
    auto new_options = options;
    if (!new_options.use_intra_process_comms()) {
      new_options.use_intra_process_comms(true);
    }
    return options;
  }

  inline rclcpp::QoS qos_from_string(const std::string &qos_name) {
    if (qos_name == "sensor_data") {
      return rclcpp::SensorDataQoS();
    }
    if (qos_name == "default") {
      return rclcpp::QoS(rclcpp::KeepLast(10));
    }
    // fallback
    return rclcpp::SensorDataQoS();
  };

  template<typename T, typename = void>
  struct has_header : std::false_type {
  };

  template<typename T>
  struct has_header<T, std::void_t<decltype(std::declval<T>().header)> > : std::true_type {
  };

  template<typename MsgT>
  int64_t get_message_timestamp_ns(const MsgT &msg, const int64_t &recv_ns) {
    if constexpr (has_header<MsgT>::value) {
      return static_cast<int64_t>(msg.header.stamp.sec) * 1000000000LL +
             static_cast<int64_t>(msg.header.stamp.nanosec);
    } else {
      // fallback: receipt time
      return recv_ns;
    }
  }

  static std::string require_string(const YAML::Node &n, const char *key) {
    if (!n[key] || !n[key].IsScalar()) {
      throw std::runtime_error(std::string("Missing/invalid key: ") + key);
    }
    return n[key].as<std::string>();
  }

  static double require_double(const YAML::Node &n, const char *key) {
    if (!n[key] || !n[key].IsScalar()) {
      throw std::runtime_error(std::string("Missing/invalid key: ") + key);
    }
    return n[key].as<double>();
  }

  static size_t require_size_t(const YAML::Node &n, const char *key) {
    if (!n[key] || !n[key].IsScalar()) {
      throw std::runtime_error(std::string("Missing/invalid key: ") + key);
    }
    return n[key].as<size_t>();
  }
}

#endif //LSY_ROS_DATA_UTILS_UTILS_HPP

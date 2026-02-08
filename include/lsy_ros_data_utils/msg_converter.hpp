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
// Created by haoming on 2/8/26.
//

#ifndef LSY_ROS_DATA_UTILS_MSG_CONVERTER_HPP
#define LSY_ROS_DATA_UTILS_MSG_CONVERTER_HPP

#include <sensor_msgs/msg/image.hpp>
#include <sensor_msgs/msg/compressed_image.hpp>

#include <opencv2/imgcodecs.hpp>
#include <opencv2/imgproc.hpp>

#include <stdexcept>
#include <string>
#include <vector>

namespace lsy_ros_data_utils {
  // Map ROS encoding to OpenCV type + channel count.
  // (We keep this minimal for speed; extend if you need more encodings.)
  inline void encoding_to_cv_type(const std::string &enc, int &cv_type, int &channels) {
    if (enc == "rgba8" || enc == "bgra8") {
      cv_type = CV_8UC4;
      channels = 4;
      return;
    }
    if (enc == "rgb8" || enc == "bgr8") {
      cv_type = CV_8UC3;
      channels = 3;
      return;
    }
    if (enc == "mono8") {
      cv_type = CV_8UC1;
      channels = 1;
      return;
    }
    throw std::runtime_error("Unsupported encoding: " + enc);
  }

  // Efficient conversion: raw Image -> CompressedImage.
  // - format: "png" or "jpeg" (or "jpg")
  // - jpeg_quality: [0..100]
  // - png_level: [0..9]  (higher = smaller, slower)

  inline sensor_msgs::msg::CompressedImage toCompressed(
    sensor_msgs::msg::Image::ConstSharedPtr in,
    const std::string &format = "png",
    int jpeg_quality = 95,
    int png_level = 3) {
    // 1) Wrap input buffer without copying.
    int cv_type = 0, channels = 0;
    encoding_to_cv_type(in->encoding, cv_type, channels);

    if (in->step == 0 || in->data.empty()) {
      std::cout << "Input image has empty data/step." << std::endl;
    }

    // rows = height, cols = width; step is bytes per row.
    const cv::Mat src(static_cast<int>(in->height),
                      static_cast<int>(in->width),
                      cv_type,
                      const_cast<uint8_t *>(in->data.data()), // OpenCV API is non-const, but we won't modify.
                      static_cast<size_t>(in->step));

    // 2) Choose encoder + params; reuse param/output buffers to reduce allocations.
    thread_local std::vector<int> params;
    thread_local std::vector<uchar> encoded;

    params.clear();
    encoded.clear();

    std::string fmt = format;
    // normalize
    if (fmt == "jpg") fmt = "jpeg";

    std::string ext;
    if (fmt == "png") {
      ext = ".png";
      // PNG compression: 0 (fastest/biggest) .. 9 (slowest/smallest)
      params.push_back(cv::IMWRITE_PNG_COMPRESSION);
      params.push_back(std::max(0, std::min(9, png_level)));
    } else if (fmt == "jpeg") {
      ext = ".jpg";
      // JPEG quality: 0..100
      params.push_back(cv::IMWRITE_JPEG_QUALITY);
      params.push_back(std::max(0, std::min(100, jpeg_quality)));
    } else {
      std::cout << "Unsupported compressed format: " << fmt << std::endl;
    }

    // 3) Encode.
    // JPEG cannot store alpha; if input is 4-channel and JPEG requested, convert to 3-channel.
    // PNG supports 1/3/4 channels, so no conversion needed there.
    if (fmt == "jpeg" && channels == 4) {
      thread_local cv::Mat tmp3;
      // Keep it fast: only convert when needed.
      if (in->encoding == "rgba8") {
        cv::cvtColor(src, tmp3, cv::COLOR_RGBA2BGR); // OpenCV default for JPEG is BGR ordering
      } else {
        // bgra8
        cv::cvtColor(src, tmp3, cv::COLOR_BGRA2BGR);
      }
      if (!cv::imencode(ext, tmp3, encoded, params)) {
        std::cout << "cv::imencode failed for JPEG." << std::endl;
      }
    } else {
      if (!cv::imencode(ext, src, encoded, params)) {
        std::cout << "cv::imencode failed." << std::endl;
      }
    }

    // 4) Fill ROS CompressedImage
    sensor_msgs::msg::CompressedImage out;
    out.header = in->header;
    out.format = fmt; // commonly "png" or "jpeg"
    out.data.assign(encoded.begin(), encoded.end());
    return out;
  }
}

#endif //LSY_ROS_DATA_UTILS_MSG_CONVERTER_HPP

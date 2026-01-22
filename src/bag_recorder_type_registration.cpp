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
// Created by haoming on 1/18/26.
//

#include "bag_recorder_component.hpp"

#include <sensor_msgs/msg/camera_info.hpp>
#include <sensor_msgs/msg/image.hpp>
#include <sensor_msgs/msg/imu.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <sensor_msgs/msg/compressed_image.hpp>
#include <sensor_msgs/msg/magnetic_field.hpp>
#include <sensor_msgs/msg/temperature.hpp>
#include <sensor_msgs/msg/nav_sat_fix.hpp>
#include <sensor_msgs/msg/time_reference.hpp>
#include <geometry_msgs/msg/pose_stamped.hpp>
#include <geometry_msgs/msg/pose_with_covariance_stamped.hpp>
#include <geometry_msgs/msg/twist_stamped.hpp>
#include <geometry_msgs/msg/twist_with_covariance_stamped.hpp>
#include <geometry_msgs/msg/transform_stamped.hpp>
#include <nav_msgs/msg/odometry.hpp>
#include <nav_msgs/msg/path.hpp>
#include <nav_msgs/msg/occupancy_grid.hpp>
#include <diagnostic_msgs/msg/diagnostic_array.hpp>
#include <tf2_msgs/msg/tf_message.hpp>
#include <std_msgs/msg/float32_multi_array.hpp>
#include <std_msgs/msg/float64_multi_array.hpp>
#include <std_msgs/msg/string.hpp>


#ifdef HAVE_LIVOX_ROS_DRIVER2
#include <livox_ros_driver2/msg/custom_msg.hpp>
#endif

#ifdef HAVE_SEPTENTRIO_GNSS_DRIVER
#include <septentrio_gnss_driver/msg/att_euler.hpp>
#include <septentrio_gnss_driver/msg/att_cov_euler.hpp>
#include <septentrio_gnss_driver/msg/aux_ant_positions.hpp>
#include <septentrio_gnss_driver/msg/bds_alm.hpp>
#include <septentrio_gnss_driver/msg/bds_nav.hpp>
#include <septentrio_gnss_driver/msg/bds_utc.hpp>
#include <septentrio_gnss_driver/msg/bds_ion.hpp>
#include <septentrio_gnss_driver/msg/diff_corr_in.hpp>
#include <septentrio_gnss_driver/msg/block_header.hpp>
#include <septentrio_gnss_driver/msg/fast_corr.hpp>
#include <septentrio_gnss_driver/msg/gal_alm.hpp>
#include <septentrio_gnss_driver/msg/gal_gst_gps.hpp>
#include <septentrio_gnss_driver/msg/gal_ion.hpp>
#include <septentrio_gnss_driver/msg/gal_nav.hpp>
#include <septentrio_gnss_driver/msg/gal_utc.hpp>
#include <septentrio_gnss_driver/msg/glo_alm.hpp>
#include <septentrio_gnss_driver/msg/glo_nav.hpp>
#include <septentrio_gnss_driver/msg/glo_time.hpp>
#include <septentrio_gnss_driver/msg/geo_fast_corr.hpp>
#include <septentrio_gnss_driver/msg/geo_nav.hpp>
#include <septentrio_gnss_driver/msg/geo_long_term_corr.hpp>
#include <septentrio_gnss_driver/msg/lt_corr.hpp>
#include <septentrio_gnss_driver/msg/gps_alm.hpp>
#include <septentrio_gnss_driver/msg/gps_ion.hpp>
#include <septentrio_gnss_driver/msg/gps_nav.hpp>
#include <septentrio_gnss_driver/msg/gps_utc.hpp>
#include <septentrio_gnss_driver/msg/meas_epoch.hpp>
#include <septentrio_gnss_driver/msg/meas_epoch_channel_type1.hpp>
#include <septentrio_gnss_driver/msg/meas_epoch_channel_type2.hpp>
#include <septentrio_gnss_driver/msg/meas_extra.hpp>
#include <septentrio_gnss_driver/msg/meas_extra_channel.hpp>
#include <septentrio_gnss_driver/msg/ntrip_client_connection.hpp>
#include <septentrio_gnss_driver/msg/ntrip_client_status.hpp>
#include <septentrio_gnss_driver/msg/pos_cov_cartesian.hpp>
#include <septentrio_gnss_driver/msg/pos_cov_geodetic.hpp>
#include <septentrio_gnss_driver/msg/pvt_cartesian.hpp>
#include <septentrio_gnss_driver/msg/pvt_geodetic.hpp>
#include <septentrio_gnss_driver/msg/receiver_status.hpp>
#include <septentrio_gnss_driver/msg/sat_info.hpp>
#include <septentrio_gnss_driver/msg/sat_visibility.hpp>
#include <septentrio_gnss_driver/msg/quality_ind.hpp>
#endif

#ifdef HAVE_MAVROS_MSGS
#include <mavros_msgs/msg/altitude.hpp>
#include <mavros_msgs/msg/actuator_control.hpp>
#include <mavros_msgs/msg/gpsinput.hpp>
#include <mavros_msgs/msg/gpsraw.hpp>
#include <mavros_msgs/msg/gpsrtk.hpp>
#include <mavros_msgs/msg/mavlink.hpp>
#include <mavros_msgs/msg/rc_in.hpp>
#include <mavros_msgs/msg/rc_out.hpp>
#include <mavros_msgs/msg/optical_flow_rad.hpp>
#include <mavros_msgs/msg/override_rc_in.hpp>
#include <mavros_msgs/msg/state.hpp>
#include <mavros_msgs/msg/radio_status.hpp>
#include <mavros_msgs/msg/thrust.hpp>
#include <mavros_msgs/msg/trajectory.hpp>
#include <mavros_msgs/msg/waypoint_list.hpp>
#include <mavros_msgs/msg/waypoint_reached.hpp>
#include <mavros_msgs/msg/estimator_status.hpp>
#include <mavros_msgs/msg/timesync_status.hpp>
#endif

#ifdef HAVE_ZED_MSGS
#include <zed_msgs/msg/health_status_stamped.hpp>
#include <zed_msgs/msg/heartbeat.hpp>
#include <zed_msgs/msg/gnss_fusion_status.hpp>
#include <zed_msgs/msg/pos_track_status.hpp>
#include <zed_msgs/msg/svo_status.hpp>
#endif


namespace lsy_ros_data_utils::rosbag {
  void
  BagRecorderComponent::init_type_registry() {
    type_registry_.clear();

    // ToDo: to be extended with all types, can we generalize this??
    // Add types ONCE here. Config strings must match exactly.

    register_type<std_msgs::msg::String>("std_msgs/msg/String");
    register_type<std_msgs::msg::Float32MultiArray>("std_msgs/msg/Float32MultiArray");
    register_type<std_msgs::msg::Float64MultiArray>("std_msgs/msg/Float64MultiArray");

    register_type<sensor_msgs::msg::Image>("sensor_msgs/msg/Image");
    register_type<sensor_msgs::msg::PointCloud2>("sensor_msgs/msg/PointCloud2");
    register_type<sensor_msgs::msg::Imu>("sensor_msgs/msg/Imu");
    register_type<sensor_msgs::msg::CompressedImage>("sensor_msgs/msg/CompressedImage");
    register_type<sensor_msgs::msg::CameraInfo>("sensor_msgs/msg/CameraInfo");
    register_type<sensor_msgs::msg::MagneticField>("sensor_msgs/msg/MagneticField");
    register_type<sensor_msgs::msg::Temperature>("sensor_msgs/msg/Temperature");
    register_type<sensor_msgs::msg::TimeReference>("sensor_msgs/msg/TimeReference");
    register_type<sensor_msgs::msg::NavSatFix>("sensor_msgs/msg/NavSatFix");

    register_type<geometry_msgs::msg::PoseStamped>("geometry_msgs/msg/PoseStamped");
    register_type<geometry_msgs::msg::PoseWithCovarianceStamped>("geometry_msgs/msg/PoseWithCovarianceStamped");
    register_type<geometry_msgs::msg::TwistStamped>("geometry_msgs/msg/TwistStamped");
    register_type<geometry_msgs::msg::TwistWithCovarianceStamped>("geometry_msgs/msg/TwistWithCovarianceStamped");
    register_type<geometry_msgs::msg::TransformStamped>("geometry_msgs/msg/TransformStamped");

    register_type<nav_msgs::msg::Path>("nav_msgs/msg/Path");
    register_type<nav_msgs::msg::OccupancyGrid>("nav_msgs/msg/OccupancyGrid");
    register_type<nav_msgs::msg::Odometry>("nav_msgs/msg/Odometry");

    register_type<diagnostic_msgs::msg::DiagnosticArray>("diagnostic_msgs/msg/DiagnosticArray");

    register_type<tf2_msgs::msg::TFMessage>("tf2_msgs/msg/TFMessage");

    // custom message
#ifdef HAVE_LIVOX_ROS_DRIVER2
    register_type<livox_ros_driver2::msg::CustomMsg>("livox_ros_driver2/msg/CustomMsg");
    register_type<livox_ros_driver2::msg::CustomPoint>("livox_ros_driver2/msg/CustomPoint");
#endif

#ifdef HAVE_SEPTENTRIO_GNSS_DRIVER
    register_type<septentrio_gnss_driver::msg::BlockHeader>("septentrio_gnss_driver/msg/BlockHeader");
    register_type<septentrio_gnss_driver::msg::AttEuler>("septentrio_gnss_driver/msg/AttCovEuler");
    register_type<septentrio_gnss_driver::msg::AttCovEuler>("septentrio_gnss_driver/msg/AttCovEuler");
    register_type<septentrio_gnss_driver::msg::AuxAntPositions>("septentrio_gnss_driver/msg/AuxAntPositions");
    register_type<septentrio_gnss_driver::msg::BDSAlm>("septentrio_gnss_driver/msg/BDSAlm");
    register_type<septentrio_gnss_driver::msg::BDSIon>("septentrio_gnss_driver/msg/BDSIon");
    register_type<septentrio_gnss_driver::msg::BDSNav>("septentrio_gnss_driver/msg/BDSNav");
    register_type<septentrio_gnss_driver::msg::BDSUtc>("septentrio_gnss_driver/msg/BDSUtc");
    register_type<septentrio_gnss_driver::msg::GALAlm>("septentrio_gnss_driver/msg/GALAlm");
    register_type<septentrio_gnss_driver::msg::GALGstGps>("septentrio_gnss_driver/msg/GALGstGps");
    register_type<septentrio_gnss_driver::msg::GALIon>("septentrio_gnss_driver/msg/GALIon");
    register_type<septentrio_gnss_driver::msg::GALNav>("septentrio_gnss_driver/msg/GALNav");
    register_type<septentrio_gnss_driver::msg::GALUtc>("septentrio_gnss_driver/msg/GALUtc");
    register_type<septentrio_gnss_driver::msg::GLOAlm>("septentrio_gnss_driver/msg/GLOAlm");
    register_type<septentrio_gnss_driver::msg::GLONav>("septentrio_gnss_driver/msg/GLONav");
    register_type<septentrio_gnss_driver::msg::GLOTime>("septentrio_gnss_driver/msg/GLOTime");
    register_type<septentrio_gnss_driver::msg::GPSAlm>("septentrio_gnss_driver/msg/GPSAlm");
    register_type<septentrio_gnss_driver::msg::GPSIon>("septentrio_gnss_driver/msg/GPSIon");
    register_type<septentrio_gnss_driver::msg::GPSNav>("septentrio_gnss_driver/msg/GPSNav");
    register_type<septentrio_gnss_driver::msg::GPSUtc>("septentrio_gnss_driver/msg/GPSUtc");
    register_type<septentrio_gnss_driver::msg::DiffCorrIn>("septentrio_gnss_driver/msg/DiffCorrIn");
    register_type<septentrio_gnss_driver::msg::BlockHeader>("septentrio_gnss_driver/msg/BlockHeader");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::GEOFastCorr>("septentrio_gnss_driver/msg/GEOFastCorr");
    register_type<septentrio_gnss_driver::msg::LTCorr>("septentrio_gnss_driver/msg/LTCorr");
    register_type<septentrio_gnss_driver::msg::GEONav>("septentrio_gnss_driver/msg/GEONav");
    register_type<septentrio_gnss_driver::msg::GEOLongTermCorr>("septentrio_gnss_driver/msg/GEOLongTermCorr");
    register_type<septentrio_gnss_driver::msg::MeasEpoch>("septentrio_gnss_driver/msg/MeasEpoch");
    register_type<septentrio_gnss_driver::msg::MeasEpochChannelType1>("septentrio_gnss_driver/msg/MeasEpochChannelType1");
    register_type<septentrio_gnss_driver::msg::MeasEpochChannelType2>("septentrio_gnss_driver/msg/MeasEpochChannelType2");
    register_type<septentrio_gnss_driver::msg::MeasExtra>("septentrio_gnss_driver/msg/MeasExtra");
    register_type<septentrio_gnss_driver::msg::MeasExtraChannel>("septentrio_gnss_driver/msg/MeasExtraChannel");
    register_type<septentrio_gnss_driver::msg::NTRIPClientConnection>("septentrio_gnss_driver/msg/NTRIPClientConnection");
    register_type<septentrio_gnss_driver::msg::NTRIPClientStatus>("septentrio_gnss_driver/msg/NTRIPClientStatus");
    register_type<septentrio_gnss_driver::msg::PosCovCartesian>("septentrio_gnss_driver/msg/PosCovCartesian");
    register_type<septentrio_gnss_driver::msg::PosCovGeodetic>("septentrio_gnss_driver/msg/PosCovGeodetic");
    register_type<septentrio_gnss_driver::msg::PVTCartesian>("septentrio_gnss_driver/msg/PVTCartesian");
    register_type<septentrio_gnss_driver::msg::PVTGeodetic>("septentrio_gnss_driver/msg/PVTGeodetic");
    register_type<septentrio_gnss_driver::msg::ReceiverStatus>("septentrio_gnss_driver/msg/ReceiverStatus");
    register_type<septentrio_gnss_driver::msg::SatInfo>("septentrio_gnss_driver/msg/SatInfo");
    register_type<septentrio_gnss_driver::msg::SatVisibility>("septentrio_gnss_driver/msg/SatVisibility");
    register_type<septentrio_gnss_driver::msg::QualityInd>("septentrio_gnss_driver/msg/QualityInd");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
    register_type<septentrio_gnss_driver::msg::FastCorr>("septentrio_gnss_driver/msg/FastCorr");
#endif

#ifdef HAVE_MAVROS_MSGS
    register_type<mavros_msgs::msg::ActuatorControl>("mavros_msgs/msg/ActuatorControl");
    register_type<mavros_msgs::msg::Altitude>("mavros_msgs/msg/Altitude");
    register_type<mavros_msgs::msg::GPSINPUT>("mavros_msgs/msg/GPSINPUT");
    register_type<mavros_msgs::msg::GPSRAW>("mavros_msgs/msg/GPSRAW");
    register_type<mavros_msgs::msg::GPSRTK>("mavros_msgs/msg/GPSRTK");
    register_type<mavros_msgs::msg::Mavlink>("mavros_msgs/msg/Mavlink");
    register_type<mavros_msgs::msg::RCIn>("mavros_msgs/msg/RCIn");
    register_type<mavros_msgs::msg::RCOut>("mavros_msgs/msg/RCOut");
    register_type<mavros_msgs::msg::OverrideRCIn>("mavros_msgs/msg/OverrideRCIn");
    register_type<mavros_msgs::msg::State>("mavros_msgs/msg/State");
    register_type<mavros_msgs::msg::RadioStatus>("mavros_msgs/msg/RadioStatus");
    register_type<mavros_msgs::msg::Thrust>("mavros_msgs/msg/Thrust");
    register_type<mavros_msgs::msg::TimesyncStatus>("mavros_msgs/msg/TimesyncStatus");
    register_type<mavros_msgs::msg::Trajectory>("mavros_msgs/msg/Trajectory");
    register_type<mavros_msgs::msg::Waypoint>("mavros_msgs/msg/Waypoint");
    register_type<mavros_msgs::msg::WaypointList>("mavros_msgs/msg/WaypointList");
    register_type<mavros_msgs::msg::WaypointReached>("mavros_msgs/msg/WaypointReached");
    register_type<mavros_msgs::msg::EstimatorStatus>("mavros_msgs/msg/EstimatorStatus");
    register_type<mavros_msgs::msg::OpticalFlowRad>("mavros_msgs/msg/OpticalFlowRad");
#endif

#ifdef HAVE_ZED_MSGS
    register_type<zed_msgs::msg::HealthStatusStamped>("zed_msgs/msg/HealthStatusStamped");
    register_type<zed_msgs::msg::Heartbeat>("zed_msgs/msg/Heartbeat");
    register_type<zed_msgs::msg::GnssFusionStatus>("zed_msgs/msg/GnssFusionStatus");
    register_type<zed_msgs::msg::PosTrackStatus>("zed_msgs/msg/PosTrackStatus");
    register_type<zed_msgs::msg::SvoStatus>("zed_msgs/msg/SvoStatus");
#endif


  }
}
cmake_minimum_required(VERSION 3.8)
project(lsy_ros_data_utils)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POLICY_VERSION_MINIMUM 3.5)

if (CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    add_compile_options(-Wall -Wextra -Wpedantic)
endif ()

# find dependencies
find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rclcpp_components REQUIRED)
find_package(rosbag2_cpp REQUIRED)
find_package(rosbag2_storage REQUIRED)
find_package(yaml-cpp REQUIRED)

# general ros messages
find_package(std_msgs REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(geometry_msgs REQUIRED)
find_package(nav_msgs REQUIRED)

set(AMENT_DEPENDENCIES_MSG
        std_msgs
        sensor_msgs
        geometry_msgs
        nav_msgs
        diagnostic_msgs
        tf2_msgs
)

# custom ros messages
foreach (msg_pkg IN ITEMS
        "livox_ros_driver2"
        "septentrio_gnss_driver"
        "mavros_msgs"
        "zed_msgs"
        "gps_msgs"
)
    find_package(${msg_pkg} QUIET)
    if (${msg_pkg}_FOUND)
        message(STATUS "Message package '${msg_pkg}' found → enabling typed support")
        list(APPEND AMENT_DEPENDENCIES_MSG ${msg_pkg})
        string(TOUPPER ${msg_pkg} MSG_PKG_UPPER)
        add_compile_definitions(HAVE_${MSG_PKG_UPPER}=1)
    else ()
        message(STATUS "Message package '${msg_pkg}' NOT found → building without typed support")
    endif ()
endforeach ()

include_directories(
        include
        include/${PROJECT_NAME}
)

set(AMENT_DEPENDENCIES_GENERAL
        rclcpp
        rclcpp_components
        rosbag2_cpp
        rosbag2_storage
)


add_library(bag_recorder_component SHARED
        src/bag_recorder_component.cpp
        src/bag_recorder_type_registration.cpp
)
target_link_libraries(bag_recorder_component
        yaml-cpp
)
target_include_directories(bag_recorder_component PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/include/${PROJECT_NAME}
)
target_compile_features(bag_recorder_component PUBLIC cxx_std_20)
ament_target_dependencies(bag_recorder_component
        ${AMENT_DEPENDENCIES_GENERAL}
        ${AMENT_DEPENDENCIES_MSG}
)

rclcpp_components_register_nodes(bag_recorder_component "lsy_ros_data_utils::rosbag::BagRecorderComponent")

install(TARGETS bag_recorder_component
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION lib/${PROJECT_NAME}
)

ament_export_include_directories(include)
ament_export_libraries(
        bag_recorder_component
)
ament_export_dependencies(${AMENT_DEPENDENCIES_GENERAL}
        ${AMENT_DEPENDENCIES_MSG})

install(DIRECTORY include/
        DESTINATION include
)
install(DIRECTORY config launch
        DESTINATION share/${PROJECT_NAME}
)
ament_package()

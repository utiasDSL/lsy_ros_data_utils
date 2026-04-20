#  Copyright (c) 2026  Learning Systems and Robotics Lab,
#  Technical University of Munich (TUM)
#
#  Authors:  Haoming Zhang <haoming.zhang@tum>
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
#

import os

from datetime import datetime 
from ament_index_python.packages import get_package_share_directory
from launch.actions import (
    DeclareLaunchArgument,
    OpaqueFunction,
    LogInfo,
    ExecuteProcess,
    TimerAction,
)
from launch import LaunchDescription
from launch_ros.actions import (
    Node,
    ComposableNodeContainer,
    LoadComposableNodes
)
from launch_ros.descriptions import ComposableNode
from launch.substitutions import (
    LaunchConfiguration,
    Command,
    TextSubstitution, 
    PathSubstitution, 
    FindExecutable
)



os.environ["RCUTILS_COLORIZED_OUTPUT"] = "1"

default_config = os.path.join(
    get_package_share_directory('lsy_ros_data_utils'),
    'config',
    'saberguide_bag_config_indoor_w_d456.yaml'
)

def call_zed_record_svo_service(svo_path):
    # Define the service call parameters
    service_name = "/zed/zed_node/start_svo_rec"
    service_type = "zed_msgs/srv/StartSvoRec"
    # Format the request as a YAML-style string
    service_request = f"'{{svo_filename: \"{svo_path}\", compression_mode: 4}}'"

    return ExecuteProcess(
        cmd=[
            FindExecutable(name='ros2'),
            " service call ",
            service_name,
            " ",
            service_type,
            " ",
            service_request
        ],
        shell=True,
        output='screen'
    )


def launch_setup(context, *args, **kwargs):
    return_array = []
    container_name = LaunchConfiguration('container_name')

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    svo_filename = f"/home/user/01_svo/zed_recording_{timestamp}.svo2"

    container_name_val = container_name.perform(context)
    if container_name_val == '':
        container_exec = 'component_container_mt'
        arguments_val = ['--ros-args', '--log-level', 'info']
        container = ComposableNodeContainer(
            name='bag_container',
            namespace='',
            package='rclcpp_components',
            executable=container_exec,
            emulate_tty=True,
            composable_node_descriptions=[],
            arguments=arguments_val,
            output='screen',
        )
        return_array.append(container)

    bag_recorder_component = ComposableNode(
        package='lsy_ros_data_utils',
        plugin='lsy_ros_data_utils::rosbag::BagRecorderComponent',
        name='bag_recorder_component',
        parameters=[{
            'config_file': default_config
        }],
        extra_arguments=[{'use_intra_process_comms': True}],
    )

    full_container_name = '/' + container_name_val
    load_composable_node = LoadComposableNodes(
        target_container=full_container_name,
        composable_node_descriptions=[bag_recorder_component]
    )
    return_array.append(load_composable_node)

    # Delay the service call by 10 seconds to ensure ZED is fully booted
    return_array.append(
        TimerAction(
            period=2.0,
            actions=[
                call_zed_record_svo_service(svo_filename)
            ]
        )
    )

    return return_array


def generate_launch_description():
    return LaunchDescription([
        DeclareLaunchArgument(
            'container_name',
            default_value='saberguide',
            description='The name of the container to be used to load the ZED component. If empty (default) a new container will be created.'),
        OpaqueFunction(function=launch_setup),

    ])

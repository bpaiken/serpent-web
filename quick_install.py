import subprocess
import sys
import os
import re


def get_package_name_from_wheel(wheel_file):
    # Extract the package name from the wheel filename
    basename = os.path.basename(wheel_file)
    package_name = re.split(r"[-_]", basename)[0]
    return package_name


def uninstall_package(package_name):
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'uninstall', '-y', package_name])
        print(f"Successfully uninstalled {package_name}")
    except subprocess.CalledProcessError:
        print(f"Failed to uninstall {package_name} or package not installed")


def install_package_from_wheel(wheel_file):
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--force-reinstall', wheel_file])
        print(f"Successfully installed package from {wheel_file}")
    except subprocess.CalledProcessError:
        print(f"Failed to install package from {wheel_file}")


def main(wheel_file):
    package_name = get_package_name_from_wheel(wheel_file)
    uninstall_package(package_name)
    install_package_from_wheel(wheel_file)


if __name__ == "__main__":
    wheel_file = "dist/serpent_web-0.1.0-py3-none-any.whl"  # Replace with your .whl file path
    main(wheel_file)

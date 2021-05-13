from utils.util import *
from utils.git_cmd import *

import commands

arrow_code_path = "/opt/Beaver/tmp/arrow"
arrow_build_dir = arrow_code_path + "/cpp/release/"
arrow_version = "17.0.0"


def arrow_deploy(beaver_env):
    arrow_clone(beaver_env)
    arrow_build(beaver_env)
    # arrow_copy_libs(beaver_env)  # this is no longer needed due to cmake will install to /lib64 dir directly.
    arrow_java_build(beaver_env)


def arrow_clone(beaver_env):
    # TODO: read from conf:
    arrow_repo = beaver_env.get("ARROW_GIT_REPO")
    arrow_branch = beaver_env.get("ARROW_BRANCH")

    if os.path.exists(arrow_code_path):
        remote_arrow_url = subprocess.check_output(
            "cd " + arrow_code_path + " && git remote -v | grep push | awk '{print $2}' ", shell=True).strip(
            '\r\n')
        if remote_arrow_url == arrow_repo:
            try:
                subprocess.check_call("cd " + arrow_code_path + " && git pull", shell=True)
            except subprocess.CalledProcessError, e:
                print e
                subprocess.check_call("rm -rf " + arrow_code_path, shell=True)
                git_clone(arrow_repo, arrow_code_path)
        else:
            subprocess.check_call("rm -rf " + arrow_code_path, shell=True)
            git_clone(arrow_repo, arrow_code_path)
    else:
        git_clone(arrow_repo, arrow_code_path)
    git_command(" checkout " + arrow_branch, arrow_code_path)
    try:
        subprocess.check_call("cd " + arrow_code_path + " && git pull", shell=True)
    except subprocess.CalledProcessError:
        print (colors.RED + beaver_env.get("ARROW_BRANCH") + " is a tag not a branch" + colors.ENDC)

def build_gcc():
    gcc_version_str = subprocess.check_output("gcc --version",shell=True).strip('\r\n')
    gcc_version = gcc_version_str.split(" ")[2]
    if int(gcc_version.split(".")[0]) < 7:
        # prepare gcc-7.3.0 for arrow-0.17.0 and llvm building
        os.system("yum -y install gmp-devel mpfr-devel libmpc-devel")
        gcc_path = "/opt/Beaver/tmp/gcc-7.3.0"
        if not os.path.isdir(gcc_path):
            if not os.path.isfile(os.path.join(package_path, "gcc-7.3.0.tar.xz")):
                print (colors.LIGHT_BLUE + "\tDownloading gcc-7.3.0.tar.xz from website..." + colors.ENDC)
                os.system("wget -P " + package_path + " https://bigsearcher.com/mirrors/gcc/releases/gcc-7.3.0/gcc-7.3.0.tar.xz")
            os.system("tar -xvf " + os.path.join(package_path, "gcc-7.3.0.tar.xz") + " -C /opt/Beaver/tmp")
            os.system("cd " + gcc_path + " && ./configure --prefix=/usr --disable-multilib && make -j$(nproc) && make install -j$(nproc)")


def arrow_build(beaver_env):
    arrow_build_cmake_command = ""
    if beaver_env.get("ARROW_DATA_SOURCE").lower() == "true":
        build_gcc()
        arrow_build_cmake_command = "cmake -DCMAKE_INSTALL_PREFIX=/usr/ " \
                                    "-DARROW_PARQUET=ON " \
                                    "-DARROW_HDFS=ON " \
                                    "-DARROW_BOOST_USE_SHARED=ON " \
                                    "-DARROW_JNI=ON " \
                                    "-DARROW_WITH_LZ4=ON " \
                                    "-DARROW_WITH_SNAPPY=ON " \
                                    "-DARROW_WITH_PROTOBUF=ON " \
                                    "-DARROW_DATASET=ON .."

    if beaver_env.get("NATIVE_SQL_ENGINE").lower() == "true":
        build_gcc()
        # deploy llvm
        llvm_path = "/opt/Beaver/tmp/llvm"
        if not os.path.isdir(llvm_path):
            if not os.path.isfile(os.path.join(package_path, "llvm-7.0.1.src.tar.xz")):
                print(colors.LIGHT_BLUE + "/tDownloading  llvm-7.0.1.src.tar.xz from website..." + colors.ENDC)
                os.system("wget -P " + package_path + " http://releases.llvm.org/7.0.1/llvm-7.0.1.src.tar.xz")
            else:
                print(colors.LIGHT_GREEN + "llvm-7.0.1.src.tar.xz has already exists in Beaver package" + colors.ENDC)
            os.system("tar xf " + os.path.join(package_path, "llvm-7.0.1.src.tar.xz") + " -C /opt/Beaver/tmp && mv /opt/Beaver/tmp/llvm-7.0.1.src " + llvm_path)

        if not os.path.isdir(os.path.join(llvm_path, "tools/clang")):
            if not os.path.isfile(os.path.join(package_path, "cfe-7.0.1.src.tar.xz")):
                print(colors.LIGHT_BLUE + "/tDownloading cfe-7.0.1.src.tar.xz from website..." + colors.ENDC)
                os.system("wget -P " + package_path + " http://releases.llvm.org/7.0.1/cfe-7.0.1.src.tar.xz")
            else:
                print(colors.LIGHT_GREEN + "cfe-7.0.1.src.tar.xz has already exists in Beaver package" + colors.ENDC)
            os.system("tar xf " + os.path.join(package_path, "cfe-7.0.1.src.tar.xz") + " -C " + os.path.join(llvm_path, "tools") + " && mv " + os.path.join(llvm_path, "tools/cfe-7.0.1.src") + " " + os.path.join(llvm_path, "tools/clang"))
        llvm_build_path = os.path.join(llvm_path, "build")
        os.system("mkdir " + llvm_build_path)
        os.system("cd " + llvm_build_path + " && cmake -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_BUILD_TYPE=Release .. && cmake --build . && cmake --build . --target install")

        # build arrow_cpp_code
        arrow_build_cmake_command = "cmake -DCMAKE_INSTALL_PREFIX=/usr/ " \
                                    "-DARROW_GANDIVA_JAVA=ON " \
                                    "-DARROW_GANDIVA=ON " \
                                    "-DARROW_PARQUET=ON " \
                                    "-DARROW_HDFS=ON " \
                                    "-DARROW_BOOST_USE_SHARED=ON " \
                                    "-DARROW_JNI=ON " \
                                    "-DARROW_WITH_LZ4=ON " \
                                    "-DARROW_WITH_SNAPPY=ON " \
                                    "-DARROW_FILESYSTEM=ON " \
                                    "-DARROW_WITH_PROTOBUF=ON " \
                                    "-DARROW_DATASET=ON " \
                                    "-DARROW_JSON=ON .."

    if beaver_env.get("OAP_with_external").lower() == "true":
        # build arrow cpp code
        arrow_build_cmake_command = "cmake -DCMAKE_BUILD_TYPE=Release " \
                                    "-DCMAKE_C_FLAGS=\"-g -O3\" " \
                                    "-DCMAKE_CXX_FLAGS=\"-g -O3\" " \
                                    "-DARROW_BUILD_TESTS=on " \
                                    "-DARROW_PLASMA_JAVA_CLIENT=on " \
                                    "-DARROW_PLASMA=on " \
                                    "-DCMAKE_INSTALL_PREFIX=/usr/ " \
                                    "-DARROW_DEPENDENCY_SOURCE=BUNDLED .. "

    subprocess.check_call("mkdir -p " + arrow_build_dir, shell=True)
    subprocess.check_call("cd " + arrow_build_dir + " && rm -rf CMakeCache.txt && " + arrow_build_cmake_command, shell=True)

    try:
        subprocess.check_call("cd " + arrow_build_dir + " && make -j$(nproc)", shell=True)
    except subprocess.CalledProcessError, e:
        print(colors.RED + "arrow build error" + colors.ENDC)

    subprocess.check_call("cd " + arrow_build_dir + " && make install -j$(nproc)", shell=True)


def arrow_copy_libs():
    # copy libs to /lib64 dir
    if os.path.exists("/lib64/libarrow.so"):
        subprocess.check_call("rm -f /lib64/libarrow.so*", shell=True)
    if os.path.exists("/lib64/libplasma.so"):
        subprocess.check_call("rm -f /lib64/libplasma.so*", shell=True)
    if os.path.exists("/lib64/libplasma_java.so"):
        subprocess.check_call("rm -f /lib64/libplasma_java.so*", shell=True)

    subprocess.check_call("cp " + arrow_build_dir + "/release/libarrow.so.17.0.0 /lib64/ ", shell=True)
    subprocess.check_call("ln -sf /lib64/libarrow.so.17.0.0 /lib64/libarrow.so.17", shell=True)
    subprocess.check_call("ln -sf /lib64/libarrow.so.17 /lib64/libarrow.so", shell=True)

    subprocess.check_call("cp " + arrow_build_dir + "/release/libplasma.so.17.0.0 /lib64/", shell=True)
    subprocess.check_call("ln -sf /lib64/libplasma.so.17.0.0 /lib64/libplasma.so.17", shell=True)
    subprocess.check_call("ln -sf /lib64/libplasma.so.17 /lib64/libplasma.so", shell=True)

    subprocess.check_call("cp " + arrow_build_dir + "/release/libplasma_java.so /lib64/", shell=True)

    subprocess.check_call("/usr/sbin/ldconfig")


def arrow_java_build(beaver_env):
    arrow_java_code_path = arrow_code_path + "/java"
    if beaver_env.get("OAP_with_external").lower() == "true":
        # build arrow-plasma.jar
        subprocess.check_call("cd " + arrow_java_code_path + " && mvn clean install -pl plasma -am -DskipTests", shell=True)
        # TODO: copy arrow-plasma-*.jar to ${SPARK_HOME}/jars/
        arrow_jar_version = "0.17.0"
        plasma_jars = "arrow-plasma-" + arrow_jar_version + ".jar"
        subprocess.check_call("rm -f " + beaver_env.get("SPARK_HOME") + "/jars/arrow-plasma*", shell=True)
        subprocess.check_call("cp " + arrow_java_code_path + "/plasma/target/" + plasma_jars + " " + beaver_env.get("SPARK_HOME") + "/jars/" + plasma_jars, shell=True)

    if beaver_env.get("NATIVE_SQL_ENGINE").lower() == "true" or beaver_env.get("ARROW_DATA_SOURCE").lower() == "true":
        subprocess.check_call("cd " + arrow_java_code_path + " && mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=" + os.path.join(arrow_build_dir, "release") + " -DskipTests", shell=True)


def start_plasma_service(master, slaves, beaver_env):
    if beaver_env.get("OAP_with_external").lower() == "true":
        # stop plasma service first
        print(colors.LIGHT_BLUE + "Stop plasma service" + colors.ENDC)
        stop_plasma_service(master, slaves, beaver_env)
        time.sleep(10)

        print (colors.LIGHT_BLUE + "Start plasma service" + colors.ENDC)
        if beaver_env.get("oap_external_config") != None:   # TODO: right?
            plasma_config = beaver_env.get("oap_external_config")
        else:
            plasma_config = " -m 15000000000 -s /tmp/plasmaStore "
        for node in slaves:
            if node.role != "master":
                plasma_file_status = ssh_execute(node, "ls /bin/plasma-store-server")
                if plasma_file_status == 0:
                    ssh_execute(node, "nohup /bin/plasma-store-server " + plasma_config + " >/dev/null 2>&1 &")
                else:
                    ssh_execute(node, "nohup /root/miniconda2/envs/oapenv/bin/plasma-store-server " + plasma_config + " >/dev/null 2>&1 &")

def stop_plasma_service(master, slaves, beaver_env):
    for node in slaves:
        ssh_execute(node, "kill -9 `pidof plasma-store-server` ")
        ssh_execute(node, "rm -f /tmp/plasmaStore")

This page contains the instruction for configuration inheritance structure:

##Why we need inheritance?
Since there are lots of configuration options in hadoop ecosystem. To remember all the configuration for each
for specified application will be very difficult. Inheritance helps to keep track of all the changes compared
with the basic common config files. It is a way for knowledge recording and sharing.

##Instruction:
**_./conf_** directory is the basic common setting that need to be inherited. Users are not suggested to use this
path directly. All the customized configuration are saved under _**./repo**_ directory. 

To start a new conf, you can copy _**./repo/conf_template**_ directory and rename it based on your purpuse. 

Each customized configuration should include a **_.base_** file which contains the parent's configuration path. You can
inherited either from the **_./conf_** path or from any other repo path that is proved to be work. 

Configurations that supported inheritance:
```
Hadoop
Hive
Spark
env
```
import os
import subprocess

#Create the autoconfig header with git revision etc
#TODO: Find a better place for autoconfig

config = {}
config["git_rev"] = subprocess.check_output(["git", "rev-parse", "HEAD"]).strip()
is_clean = subprocess.call(["git", "diff-index", "--quiet", config["git_rev"]]) == 0
config["git_clean_version"] = 1 if is_clean else 0
with open("YakServer/include/autoconfig.h", "w") as configfile:
        configfile.write("""#ifndef AUTOCONFIG_H
#define SERVER_VERSION "YakDB 0.1 alpha"
#define AUTOCONFIG_H
#define GIT_REVISION "%(git_rev)s"
#define GIT_CLEAN_VERSION %(git_clean_version)s // == 0 means there had been uncomitted changes
#endif //AUTOCONFIG_H
"""%config)

#Build C++ client library (which is a dependency of the server)
SConscript(dirs='YakClient', variant_dir='clientbuild', src_dir='YakClient', duplicate=0)
#Build server
SConscript(dirs='YakServer', variant_dir='build', src_dir='YakServer', duplicate=0)

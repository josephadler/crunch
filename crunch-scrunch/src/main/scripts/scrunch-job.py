#!/usr/bin/python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import glob
import os
import re
import shutil
import subprocess
import sys

# Configuration in script
##############################################################
if not "SCALA_HOME" in os.environ:
  sys.stderr.write("Environment variable SCALA_HOME must be set\n")
  sys.exit(1)
SCALA_LIB = os.path.join(os.environ["SCALA_HOME"], "lib")

if not "HADOOP_HOME" in os.environ:
  sys.stderr.write("Environment variable HADOOP_HOME must be set\n")
  sys.exit(1)
HADOOP_HOME = os.environ["HADOOP_HOME"]
HADOOP_JARS = ":".join(glob.glob(os.path.join(HADOOP_HOME, "*.jar")))

#Get the absolute path of the original (non-symlink) file.
if os.path.islink(__file__):
  ORIGINAL_FILE = os.readlink(__file__)
else:
  ORIGINAL_FILE = __file__

DIST_ROOT = os.path.abspath(os.path.dirname(ORIGINAL_FILE)+"/../")
LIB_DIR = DIST_ROOT + "/lib" # Dir with all scrunch dependencies.
TMPDIR = "/tmp"
BUILDDIR = TMPDIR + "/script-build"
COMPILE_CMD = "java -cp %s/scala-library.jar:%s/scala-compiler.jar -Dscala.home=%s scala.tools.nsc.Main" % (SCALA_LIB, SCALA_LIB, SCALA_LIB)
##############################################################

argv = sys.argv[1:]
if len(argv) < 1:
  sys.stderr.write("ERROR: insufficient args.\n")
  sys.exit(1)

JOBFILE = argv.pop(0)

def file_type():
  m = re.search(r'\.(scala|java)$', JOBFILE)
  if m:
    return m.group(1)
  return None

def is_file():
  return file_type() is not None

PACK_RE = r'package ([^;]+)'
OBJECT_RE = r'object\s+([^\s(]+).*(extends|with)\s+PipelineApp.*'
EXTENSION_RE = r'(.*)\.(scala|java)$'

#Get the name of the job from the file.
#the rule is: last class in the file, or the one that matches the filename
def get_job_name(file):
  package = ""
  job = None
  default = None
  match = re.search(EXTENSION_RE, file)
  if match:
    default = match.group(1)
    for s in open(file, "r"):
      mp = re.search(PACK_RE, s)
      mo = re.search(OBJECT_RE, s)
      if mp:
        package = mp.group(1).trim() + "."
      elif mo:
        if not job or not default or not job.tolower() == default.tolower():
          #use either the last class, or the one with the same name as the file
          job = mo.group(1)
    if not job:
      raise "Could not find job name"
    return "%s%s" % (package, job)
  else:
    return file

LIB_PATH = os.path.abspath(LIB_DIR)
if not os.path.exists(LIB_PATH):
  sys.stderr.write("Scrunch distribution lib directory not found; run mvn package to construct a distribution to run examples from.\n")
  sys.exit(1)
LIB_JARS = glob.glob(os.path.join(LIB_PATH, "*.jar"))
LIB_CP = ":".join(LIB_JARS)

JOBPATH = os.path.abspath(JOBFILE)
JOB = get_job_name(JOBFILE)
JOBJAR = JOB + ".jar"
JOBJARPATH = os.path.join(TMPDIR, JOBJAR)

def needs_rebuild():
  return not os.path.exists(JOBJARPATH) or os.stat(JOBJARPATH).st_mtime < os.stat(JOBPATH).st_mtime

def build_job_jar():
  sys.stderr.write("compiling " + JOBFILE + "\n")
  if os.path.exists(BUILDDIR):
    shutil.rmtree(BUILDDIR)
  os.makedirs(BUILDDIR)
  cmd = "%s -classpath %s:%s -d %s %s" % (COMPILE_CMD, LIB_CP, HADOOP_JARS, BUILDDIR, JOBFILE)
  print cmd
  if subprocess.call(cmd, shell=True):
    shutil.rmtree(BUILDDIR)
    sys.exit(1)

  jar_cmd = "jar cf %s -C %s ." % (JOBJARPATH, BUILDDIR)
  subprocess.call(jar_cmd, shell=True)
  shutil.rmtree(BUILDDIR)

def hadoop_command():
  return "HADOOP_CLASSPATH=%s ; %s/bin/hadoop jar %s %s %s" % (LIB_CP, HADOOP_HOME, JOBJARPATH, JOB, " ".join(argv))

if is_file() and needs_rebuild():
  build_job_jar()

SHELL_COMMAND = hadoop_command()
print SHELL_COMMAND
os.system(SHELL_COMMAND)

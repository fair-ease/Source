
def check_py_version():
  from platform import python_version
  print("Python version: ",python_version())


def check_modules_installed():
  from variables import work_dir
  import importlib.metadata

  requirements_file = work_dir + '/docker/requirements.txt'
  open_file = open(requirements_file,'r')
  lines = open_file.readlines()
  
  modules = set()
  for line in lines:
    modules.add(line.rstrip())

  mod_installed = sorted(x.name for x in importlib.metadata.distributions())

  required = modules
  installed = set(mod_installed)
  #installed = {pkg.key for pkg in pkg_resources.working_set}
  #print(installed)
  missing = required - installed

  print("The check is based on the 'requirements.txt' for the creation of an docker image !!")
  for mod in sorted(missing):
    print("you need to install: ",mod, " --  pip install",mod)


def modules_install():
  from variables import work_dir
  import importlib.metadata
  import subprocess

  requirements_file = work_dir + '/docker/requirements.txt'
  open_file = open(requirements_file,'r')
  lines = open_file.readlines()
  
  modules = set()
  for line in lines:
    modules.add(line.rstrip())

  mod_installed = sorted(x.name for x in importlib.metadata.distributions())

  required = modules
  installed = set(mod_installed)
  #installed = {pkg.key for pkg in pkg_resources.working_set}
  #print(installed)
  missing = required - installed

  print("The installation is based on the 'requirements.txt' for the creation of an docker image !!")
  for mod in sorted(missing):
    #print("you need to install: ",mod, " --  pip install",mod)
    subprocess.check_call(['pip', 'install', mod])


def check_directory(directory):
  import os
  if not os.path.exists(directory):
    os.makedirs(directory)
    print("created directory :",directory)


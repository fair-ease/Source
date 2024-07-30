
from variables import work_dir
from SOURCE.tools import checks


print()

# check if the module checks is installed
print("dir(checks) :",dir(checks))
print()

# check the python version
checks.check_py_version()
print()

# check the modules installed
checks.check_modules_installed()
print()

# install all the modules required
#checks.modules_install()
print()



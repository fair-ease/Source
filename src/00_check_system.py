
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

print("if you like to install automatically all the module, edit the file src/00_check_system and uncomment the line with the command 'checks.modules_install()'")
# install all the modules required
#checks.modules_install()
print()

print("Check / Installations completed !!")

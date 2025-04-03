OS=$( hostnamectl | awk '/Operating System/{ print $3" "$4 }' )
echo "Setting up SLURP for ${OS}"

# This is the directory of the script (but no symlinks allowed)
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Slurp sys: ${SCRIPT_DIR}"

source /opt/sphenix/core/bin/sphenix_setup.sh -n new
export PATH=${PATH}:${HOME}/bin:./bin
export ODBCINI=./odbc.ini

if [[ $OS =~ "Alma" ]]; then
   export PATH=/usr/bin:${PATH}
   export PYTHONPATH=/opt/sphenix/core/lib/python3.9/site-packages
   alias python=/usr/bin/python
   alias pip=/opt/sphenix/core/bin/pip3.9
fi

export PYTHONPATH=${PYTHONPATH}:${SCRIPT_DIR}/slurp
export PATH=${PATH}:${SCRIPT_DIR}/bin

echo Using $(python --version)

# parse_git_branch() {
# #  branch=$( git -C ${SCRIPT_DIR} branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/(\1)/' )
#   branch=$( git -C ${SCRIPT_DIR} branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/\1/' )
#   status="\e[31m" # red is bad
#   if [ -z "$(git -C ${SCRIPT_DIR} status --porcelain)" ]; then
#       status="\e[32m" # green is good
#   fi
#   pbranch=""
#   pstatus=""
#   if [ -e ProdFlow ]; then
#      pbranch=$( git -C ./ProdFlow branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/\1/' )
#      pstatus="\e[31m"
#      if [ -z "$(git -C ./ProdFlow status --porcelain)" ]; then
#         pstatus="\e[32m"     
#      fi
#   else
#      pbranch="NONE"
#      pstatus="\e[31m"
#   fi
  
#   echo -e " ${status}[slurp:${branch}] ${pstatus}[prodflow:${pbranch}] "
# }

#PS1="\[\e[36m\]\u@\h \[\e[32m\]\w\[\e[33m\]\$(parse_git_branch)\[\e[0m\] $ "
#PS1="\[\e[36m\]\u \[\e[32m\]\w \[\e[33m\] slurp:\$(parse_git_branch)\[\e[0m\] $ "
#PS1="\[\e[36m\]\u@\h\[\e[33m\]\$(parse_git_branch)\[\e[32m\]\[\e[34m\]\n[\W/]> \[\e[0m\]"
# PS1="\[\e[36m\]\u@\h\[\e[33m\]\$(parse_git_branch)\[\e[32m\]"
PS1="\[\e[36m\]me@\h\[\e[33m\]\[\e[34m\][\W/]> \[\e[0m\]"


if ! echo "$SHELL" | grep -q "bash"; then
   echo "This script must be run in bash"
   return 1
fi

OS=$( hostnamectl | awk '/Operating System/{ print $3" "$4 }' )
if ! [[ "$OS" =~ "Alma" || "$OS" =~ "CentOS" || "$OS" =~ "Rocky" || "$OS" =~ "RHEL" || "$OS" =~ "Ubuntu" ]]; then
   echo "This script must be run on a supported OS"
   return 1
fi
echo "Setting up sPHENIX Production for ${OS}"

# This is the directory of the script (but no symlinks allowed)
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Using scripts in sys: ${SCRIPT_DIR}"

source /opt/sphenix/core/bin/sphenix_setup.sh -n new
export PATH=${PATH}:${HOME}/bin:./bin
export ODBCINI=./odbc.ini

if [[ $OS =~ "Alma" ]]; then
   export PATH=/usr/bin:${PATH}
   export PYTHONPATH=/opt/sphenix/core/lib/python3.9/site-packages
   alias python=/usr/bin/python
   alias pip=/opt/sphenix/core/bin/pip3.9
fi

#export PYTHONPATH=${PYTHONPATH}:${SCRIPT_DIR}/slurp
#export PATH=${PATH}:${SCRIPT_DIR}/bin
export PYTHONPATH=${PYTHONPATH}:${SCRIPT_DIR}
export PATH=${PATH}:${SCRIPT_DIR}

echo Using $(python --version)

parse_git_branch() {
  branch=$( git -C ${SCRIPT_DIR} branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/\1/' )
  status="\e[31m" # red is bad
  if [ -z "$(git -C ${SCRIPT_DIR} status --porcelain)" ]; then
      status="\e[32m" # green is good
  fi
  pbranch=""
  pstatus=""
  if [ -e ProdFlow ]; then
     pbranch=$( git -C ./ProdFlow branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/\1/' )
     pstatus="\e[31m"
     if [ -z "$(git -C ./ProdFlow status --porcelain)" ]; then
        pstatus="\e[32m"
     fi
  else
     pbranch="NONE"
     pstatus="\e[31m"
  fi
  
  #echo -e " ${status}[slurp:${branch}] ${pstatus}[prodflow:${pbranch}] "
  echo -e " ${status}[slurp:${branch}] "
}

#PS1="\[\e[36m\]\u\[\e[33m\]\$(parse_git_branch)\[\e[32m\]\[\e[34m\][\W/]> \[\e[0m\]"
PS1="\[\e[36m\]me@\h\[\e[33m\]\[\e[1:34m\][\W/]> \[\e[0m\]"

# Specialized settings for individual users of sphnxbuild
# Identified from ssh agent forwarding
if [[ `ssh-add -l` =~ "kolja" ]] ; then
    echo "Hello Kolja"    
    git config --global user.name "Kolja Kauder"
    git config --global user.email "kkauder@gmail.com"    
    #git config --global push.default simple    
fi


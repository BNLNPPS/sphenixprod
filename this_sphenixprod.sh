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
export ODBCINI=./.odbc.ini

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
   branch=$( git -C ${SCRIPT_DIR} rev-parse --abbrev-ref HEAD  2> /dev/null )
   local branch_color_status="\e[31m" # red is bad
   if [ -z "$(git -C ${SCRIPT_DIR} status --porcelain -uno)" ]; then
      branch_color_status="\e[32m" # green is good
   fi
   pbranch=""
   pstatus=""
   if [ -e ProdFlow ]; then
      pbranch=$( git -C ./ProdFlow rev-parse --abbrev-ref HEAD  2> /dev/null )
      pstatus="\e[31m"
      if [ -z "$(git -C ./ProdFlow status --porcelain -uno)" ]; then
         pstatus="\e[32m"
      fi
   else
      pbranch=""
      pstatus="\e[31m"
  fi
  
   gitstatus="${branch_color_status} prod:${branch}\e[0m"
   if [ -n "$pbranch" ]; then
      #status="${status} \e[0;33mprodflow:${pbranch}\e[0m"
      gitstatus="${gitstatus} ${pstatus} prodflow:${pbranch}"
   fi
   echo -e "${gitstatus}\e[0m"
   #echo -e "\[${branch_color_status}\]prod:${branch}\[\e[0m\] \[${pstatus}\]prodflow:${pbranch}\[\e[0m\]"
}

if [[ "$-" == *i* ]]; then
   # echo "Interactive shell"
   : # nop
else
   echo "Non-interactive shell"
   return 0
fi

#aliases
alias cqb='condor_q -batch'
alias gristory='history | grep -v istory| grep $@'

# PS1="\u@\h $(parse_git_branch) \W> "
PS1="\u@\h \W> "

# Specialized settings for individual users of sphnxbuild
# Identified from ssh agent forwarding
if [[ `ssh-add -l` =~ "kolja" ]] ; then
    echo "Hello Kolja"    
    git config --global user.name "Kolja Kauder"
    git config --global user.email "kkauder@gmail.com"    
    #git config --global push.default simple
fi


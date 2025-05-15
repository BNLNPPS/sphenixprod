if echo "$SHELL" | grep -q bash ; then
   UsingBash="BashTrue"
fi
if echo "$SHELL" | grep -q zsh ; then
   UsingZsh="ZshTrue"
fi
if [ -z "$UsingBash" ] && [ -z "$UsingZsh" ]; then
   echo "This script must be run in bash or zsh"
   return 1
fi

if [ $UsingBash ]; then
   echo "Running in bash, presumably in docker. Setting up venvdocker"
   if [ -z "$VIRTUAL_ENV" ]; then
      source /Users/eickolja/sphenix/venvdocker/bin/activate
   else
      echo "Using existing virtual environment: $VIRTUAL_ENV"
   fi
fi

if [ $UsingZsh ]; then
   echo "Running in zsh. Setting up venvsphenix"
   if [ -z "$VIRTUAL_ENV" ]; then
      source /Users/eickolja/sphenix/venvsphenix/bin/activate
   else
      echo "Using existing virtual environment: $VIRTUAL_ENV"
   fi
fi

echo "Setting up Test environment for sPHENIX Production on a Mac possibly inside Docker on Mac)."
# echo WARNING: This script is meant for testing on a MacOS system without connection to the production system
# echo WARNING: It is not meant for, and will not work in, production use.

# This is the directory of the script (but no symlinks allowed)
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Using scripts in: ${SCRIPT_DIR}"

export PATH=${PATH}:${HOME}/bin:./bin
export ODBCINI=./odbc.ini
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
  
   # The output needs to include the leading space and the yellow color,
   # and ensure all internal ANSI codes are bracketed for correct prompt width calculation.
   gitstatus="${branch_color_status} prod:${branch}\e[0m"
   if [ -n "$pbranch" ]; then
      #status="${status} \e[0;33mprodflow:${pbranch}\e[0m"
      gitstatus="${gitstatus} ${pstatus} prodflow:${pbranch}"
   fi
   echo -e "${gitstatus}\e[0m"
   # echo -e "${branch_color_status} prod:${branch} ${pstatus} prodflow:${pbranch} \e[0m"
}

if [[ -n "$BASH_VERSION" ]]; then
   # Bash prompt
   #PS1="\u@\h \$(parse_git_branch)\W> "
   PS1="\e[36m\u@\h \e[33m\W>\e[0m "
   git config --global user.name "Kolja Kauder"
   git config --global user.email "kkauder@gmail.com"    
elif [[ -n "$ZSH_VERSION" ]]; then
   # Zsh prompt
   PS1="%{[36m%}me@%m%{[33m%}%{[1;33m%} [%1~/]> %{[0m%}"
   #PS1='%n@%m$(parse_git_branch) %1~> '
fi


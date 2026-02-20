if ! echo "$SHELL" | grep -q "bash"; then
   echo "This script must be run in bash"
   return 1
fi

OS=$( hostnamectl | awk '/Operating System/{ print $3" "$4 }' )
# if ! [[ "$OS" =~ "Alma" || "$OS" =~ "CentOS" || "$OS" =~ "Rocky" || "$OS" =~ "RHEL" || "$OS" =~ "Ubuntu" ]]; then
#    echo "This script must be run on a supported OS"
#    return 1
# fi
echo "Setting up sPHENIX Production for ${OS}"

# This is the directory of the script (but no symlinks allowed)
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Using scripts in sys: ${SCRIPT_DIR}"

source /opt/sphenix/core/bin/sphenix_setup.sh -n new
export PATH=${PATH}:${HOME}/bin:./bin
export ODBCINI=./.odbc.ini

export PYTHONPATH=${PYTHONPATH}:${SCRIPT_DIR}
export PATH=${PATH}:${SCRIPT_DIR}

echo Using $(python --version)

parse_git_branch() {
    #branch name
    branch=$( git -C ${SCRIPT_DIR} rev-parse --abbrev-ref HEAD  2> /dev/null )

    local branch_color_status="\e[31m" # red is dangerous
    if [[ "$branch" == "main" ]] ; then
        branch_color_status="\e[42m" # Blue is safe (green (31) is ugly)
    fi

    # bold font if there are uncommitted changes
    if [ -z "$(git -C ${SCRIPT_DIR} status --porcelain -uno)" ]; then
        : # nop
    else
        branch_color_status="$branch_color_status\e[1m"
    fi
    gitstatus="${branch_color_status} git:${branch}\e[0m"

#    pbranch=""
#    pstatus=""
#    if [ -e ProdFlow ]; then
#       pbranch=$( git -C ./ProdFlow rev-parse --abbrev-ref HEAD  2> /dev/null )
#       pstatus="\e[31m"
#       if [ -z "$(git -C ./ProdFlow status --porcelain -uno)" ]; then
#          pstatus="\e[32m"
#       fi
#    else
#       pbranch=""
#       pstatus="\e[31m"
#   fi

    # if [ -n "$pbranch" ]; then
    #     #status="${status} \e[0;33mprodflow:${pbranch}\e[0m"
    #     # gitstatus="${gitstatus} ${pstatus} prodflow:${pbranch}"
    #     gitstatus="${gitstatus} ${pstatus}"
    # fi

    #echo -e "\[${branch_color_status}\]prod:${branch}\[\e[0m\] \[${pstatus}\]prodflow:${pbranch}\[\e[0m\]"
    echo -e "${gitstatus}\e[0m"
}
#Could also use PROMPT_COMMAND=parse_git_branch
#PS1="\u@\h $(parse_git_branch) \W> "
PS1="\h:\w\$(parse_git_branch)> "

# PS1="\u@\h \W> "

if [[ "$-" == *i* ]]; then
   # echo "Interactive shell"
   : # nop
else
   echo "Non-interactive shell"
   return 0
fi

#aliases
alias cqb='condor_q -batch'
alias cs='condor_submit'
alias gristory='history | grep -v istory| grep $@'
alias rehash='hash -r'

# Specialized settings for individual users of sphnxpro
# Identified from ssh agent forwarding
if [[ `ssh-add -l` =~ "eickolja" ]] ; then
    echo "Hello Kolja"
    export GIT_CONFIG_GLOBAL=/sphenix/u/sphnxpro/.gitconfig.kolja
    git config --global user.name "Kolja Kauder"
    git config --global user.email "kkauder@gmail.com"
    #git config --global push.default simple
    git config --global push.autoSetupRemote true

    if [[ -n "$_CONDOR_SCRATCH_DIR" ]]; then
	echo "_CONDOR_SCRATCH_DIR = $_CONDOR_SCRATCH_DIR"
    else
	echo "_CONDOR_SCRATCH_DIR is not defined."
	export _CONDOR_SCRATCH_DIR=~/devkolja/condorscratch
	echo " ... now set to $_CONDOR_SCRATCH_DIR"
    fi

    # zsh-style history search
    bind '"\e[A": history-search-backward'
    bind '"\e[B": history-search-forward'

fi


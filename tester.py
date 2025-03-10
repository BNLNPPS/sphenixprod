import cProfile

from simpleLogger import * 
from ruleclasses import RuleConfig

from argparsing import submit_args
# Keep arguments and the parser global so we can access them everywhere and 
# so submodules can add to them a la
# https://mike.depalatis.net/blog/simplifying-argparse
args     = None
userargs = None


# ============================================================================================
def main():

    args = submit_args()
    yname = args.config

    try:
        # Load all rules from the given yaml file.
        all_rules = RuleConfig.from_yaml_file(yname)
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error: {e}")
        exit(1)
    
    for rule_name, rule_config in all_rules.items():
        INFO(f"Successfully loaded rule configuration: {rule_name}")
        # print(rule_config.dict())
        # INFO("---------------------------------------")



# ============================================================================================
if __name__ == '__main__':
    main()
    exit(0)

    cProfile.run('main()')

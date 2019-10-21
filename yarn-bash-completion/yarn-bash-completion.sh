#!/bin/bash

#
# Generate completions for common user commands
#

_yarn_subcommands=( jar application applicationattempt container node queue logs classpath top )

_yarn_application_list()
{
    yarn application -list -appStates ALL 2>/dev/null | grep -Po -e '^application_[\d]+_[\d]+'
}

_yarn_attempt_list()
{
    local application_id=$1
    yarn applicationattempt -list "${application_id}" 2>/dev/null | grep -Po -e '^appattempt_[\d]+_[\d]+_[\d]+'
}

_yarn_container_list()
{
    local attempt_id=$1
    yarn container -list ${attempt_id} 2>/dev/null | grep -Po -e 'container_[\d]+_[\d]+_[\d]+_[\d]+'
}

_yarn_node_list()
{
    yarn node -list -all 2>/dev/null | \
        grep -Po -e "^(([a-z0-9]|[a-z0-9][-a-z0-9]*[a-z0-9])[.])*([a-z0-9]|[a-z0-9][-a-z0-9]*[a-z0-9])[:][1-9][0-9]{1,6}(?=[[:space:]])"
}

_yarn_complete_logs()
{
    local curr_word="${COMP_WORDS[COMP_CWORD]}"
    local prev_word="${COMP_WORDS[COMP_CWORD-1]}"
    
    #echo "_yarn_complete_logs: <${COMP_WORDS[*]:2}> prev_word=${prev_word} curr_word=${curr_word}" >>/tmp/yarn-completion.log 
    
    COMPREPLY=()
    
    if [[ ${curr_word} =~ ^- ]]; then
        COMPREPLY=( $(compgen -W "-applicationId -containerId -appOwner" -- ${curr_word}) )
    elif [[ ${prev_word} =~ ^- ]]; then
        case "${prev_word#-}" in
            applicationId)
                local application_ids=$(_yarn_application_list)
                COMPREPLY=( $(compgen -W "${application_ids}" -- ${curr_word}) )
            ;;
            containerId)
                # If applicationId is specified before, suggest only containers belonging to the application
                local container_ids=()
                local application_ids=() 
                local application_id=$(echo ${COMP_WORDS[*]:1} | \
                    grep -Po -e '(?<=[[:space:]][-]applicationId)[[:space:]]+application_[\d]+_[\d]+(?=$|[[:space:]])' | \
                    tr -d '[:space:]')
                if [[ -z "${application_id}" ]]; then
                    application_ids=( $(_yarn_application_list) )
                else
                    application_ids=( "${application_id}" )
                fi
                for id in ${application_ids[*]:0:3}; do
                    for attempt_id in $(_yarn_attempt_list ${id}); do
                        container_ids+=( $(_yarn_container_list ${attempt_id}) )
                    done
                done
                COMPREPLY=( $(compgen -W "${container_ids[*]}" -- ${curr_word}) )
            ;;
            appOwner)
                COMPREPLY=( $(compgen -u -- ${curr_word}) )
            ;;
        esac
    fi
}

_yarn_complete_node()
{
    local states=( NEW RUNNING UNHEALTHY DECOMMISSIONED LOST REBOOTED DECOMMISSIONING SHUTDOWN )
    
    local curr_word="${COMP_WORDS[COMP_CWORD]}"
    local prev_word="${COMP_WORDS[COMP_CWORD-1]}"
    
    #echo "_yarn_complete_node: <${COMP_WORDS[*]:2}> prev_word=${prev_word} curr_word=${curr_word}" >>/tmp/yarn-completion.log 

    COMPREPLY=()
    if [[ ${curr_word} =~ ^- ]]; then
        COMPREPLY=( $(compgen -W "-list -all -showDetails -status -states" -- ${curr_word}) )
    elif [[ ${prev_word} =~ ^- ]]; then
        case "${prev_word#-}" in
            status)
                local node_ids=( $(_yarn_node_list) )
                COMPREPLY=( $(compgen -W "${node_ids[*]}" -- ${curr_word}) )
            ;;
            states)
                COMPREPLY=( $(compgen -W "${states[*]}" -- ${curr_word}) )
            ;;
        esac
    fi
}

_yarn_complete_application()
{
    # Todo
    COMPREPLY=()
}

_yarn_complete_applicationattempt()
{
    # Todo
    COMPREPLY=()
}

_yarn_complete_container()
{
    # Todo
    COMPREPLY=()
}

_yarn_complete_jar()
{
    # Use generic options documented at: 
    # https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CommandsManual.html#Generic_Options
    # Todo
    COMPREPLY=()
}

_yarn_complete() 
{
    #echo "COMP_CWORD=${COMP_CWORD} COMP_WORDS=${COMP_WORDS[*]}" >>/tmp/yarn-completion.log 
    
    local curr_word="${COMP_WORDS[COMP_CWORD]}"
    
    if (( COMP_CWORD == 1 )); then
        COMPREPLY=( $(compgen -W "${_yarn_subcommands[*]}" -- ${curr_word}) )
    else
        # Switch based on 1st word (i.e. the subcommand)
        case ${COMP_WORDS[1]} in
            application)
                _yarn_complete_application
            ;;
            applicationattempt)
                _yarn_complete_applicationattempt
            ;;
            jar)
                _yarn_complete_jar
            ;;
            container)
                _yarn_complete_container
            ;;
            node)
                _yarn_complete_node
            ;;
            queue)
            ;;
            logs)
                _yarn_complete_logs
            ;;
        esac
    fi
}

complete -F _yarn_complete yarn



#!/usr/bin/env bash

if [ -n "$1" ]; then
    OPERATION="$1"
else
    OPERATION="apply"
fi

if [ -z "${REPOSITORY_PREFIX}" ]
then 
    echo "Please set the REPOSITORY_PREFIX"
else 
    for config in $(ls ./eks/*.yaml)
    do
        sed  -e 's#\${REPOSITORY_PREFIX}'"#${REPOSITORY_PREFIX}#g" -e 's#\${MYSQL_PASSWORD}'"#${MYSQL_PASSWORD}#g" ${config} | kubectl ${OPERATION} -f -
    done
fi
#!/bin/bash

USER_NAME='zzzzer'
HOME_PATH="/home/${USER_NAME}"
PYTHON_VERSION='3.7.3'
PYTHON_PATH="${HOME_PATH}/.pyenv/versions/${PYTHON_VERSION}/bin"
CODE_PATH="$(cd "$(dirname $0)";pwd)"
LOG_PATH="${CODE_PATH}/log"

# 如果文件夹不存在，创建文件夹
if [ ! -d "${LOG_PATH}" ]; then
  mkdir "${LOG_PATH}"
fi

script_array=('football_match_schedule' 'football_match' 'football_bet' 'basketball_match_schedule' 'basketball_match' 'basketball_bet' 'betfair' 'betfair_detail')

# script_array[@] 会获取所有数组成员
for var in "${script_array[@]}"; do
   ${PYTHON_PATH}/python "${CODE_PATH}/${var}.py" &>> "${LOG_PATH}/${var}.log"
done
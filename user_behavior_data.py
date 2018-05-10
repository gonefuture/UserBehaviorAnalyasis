# -*- coding: utf-8 -*-
# @Author  : 钱伟健 gonefuture@qq.com
# @Time    : 2018/3/9 10:29
import datetime
import random
import time

import os
from flask import json


class TestData:
    @staticmethod
    def user_behavior():
        time.sleep(0.05)  # 0.05毫秒的延时,防止userId重复
        current_time = int(time.time())  # 当前时间的结束时间
        start_time = random.randint(current_time - 86400, current_time-100)
        end_time = random.randint(start_time, current_time)
        all_activetime = end_time - start_time + 10
        data = []
        for i in range(random.randint(1, 10)):
            data.append({
                'package': 'com.app' + str(i),
                'activetime': random.randint(1, all_activetime)
            })

        msg = {
            'userId':
            #datetime.datetime.now().strftime('%H%M%S%f') +
            str(random.randint(1, 100000)),
            'day':
            time.strftime('%Y-%m-%d', time.localtime(time.time())),
            'begintime':
            start_time,
            'endtime':
            end_time,
            'data':
            data
        }
        return msg


if __name__ == '__main__':
    test_data = TestData()
    count = 1
    print("======================开始产生用户行为数据====================")
    if not os.path.exists('./behavior'):
        print("新建文件夹 behavior")
        os.mkdir('./behavior')
    while True:
        print("现在产生第" + str(count) + "个数据")
        file_name = './behavior/' + datetime.datetime.now().strftime(
            '%Y-%m-%d-%H-%M-%S') + '--' + str(count) + '.log'

        # 一个日志文件随机可能有100到800条数据
        for i in range(random.randint(100, 300)):
            with open(file_name, 'a', encoding='utf-8', newline=None) as json_file:
                json.dump(
                    test_data.user_behavior(), json_file, ensure_ascii=False)
                json_file.write('\n')
        count = count + 1
        time.sleep(10)

import os
from multiprocessing import JoinableQueue, Value, Process

from bson import ObjectId
from playwright.sync_api import sync_playwright

from config import config
from crawler.comics18 import Comics18
from mongo import mongo

import pprint

conf = config.Configure()
comics18_crawler = Comics18(conf)
comics18_config = conf.data["comics18"]
max_proc = comics18_config["maxDownloadProc"]


def task_producer(joinable_queue, mp_counter):
    comics18_crawler.produce_comic_download_task(joinable_queue, mp_counter)
    joinable_queue.join()


def task_consumer(joinable_queue, mp_counter):
    while True:
        download_item = joinable_queue.get()
        name = download_item['title']
        print("start working on: ", name)
        comics18_crawler.consume_comics_download_task(download_item)
        with mp_counter.get_lock():
            mp_counter.value -= 1
        joinable_queue.task_done()


def get_detail_info(proc_id):
    comics18_crawler.get_detail_info(proc_id)

def update_fields():
    db_conf = conf.data['db']
    db_inst = mongo.Mongo(db_conf["host"], db_conf["port"], db_conf["db_name"])
    coll_name = "comics_18"
    db_cursor = db_inst.find_all(coll_name,{},sort_key="love_cnt",sort_val=-1).limit(2)
    comics_list = [item for item in db_cursor]
    for comic_item in comics_list:
        #pprint.pprint(comic_item)
        doc_id = comic_item["_id"]
        jmid = comic_item["jmid"].replace("\n", "").replace("禁漫車：", "").strip()
        page_text = int(comic_item["page_text"])
        view_count = comic_item["view_count"]
        view_count = process_abbr(view_count)
        click_count = str(comic_item["click_count"]).replace("\n", "").replace("點擊喜歡", "")
        print(click_count)
        click_count = process_abbr(click_count)
        love_cnt = comic_item["love_cnt"]
        love_cnt = process_abbr(love_cnt)
        comic_desc = comic_item["comic_desc"].replace("\n", "").strip()
        db_inst.update_one(coll_name, {"_id": doc_id},
                           {"$set": {"jmid": jmid, "view_count": view_count, "page_text": page_text,
                                     "click_count": click_count, "love_cnt": love_cnt, "comic_desc": comic_desc}})

def process_abbr(item):
    if isinstance(item, int):
        return item
    if item.strip() == "":
        return 0
    if "K" in item:
        item = float(item.replace("K", "")) * 1000
    elif "W" in item:
        item = float(item.replace("W", "")) * 10000
    elif "M" in item:
        item = float(item.replace("M", "")) * 1000000
    print(item)
    return int(item)


if __name__ == '__main__':
    # 获取漫画基础信息
    comics18_crawler.get_base_info()
    task_list = []
    start = 1
    for proc in range(10):
        c = Process(target=get_detail_info, args=(start,), daemon=True)
        task_list.append(c)
        start += 1
        c.start()
    for c in task_list:
        c.join()
    update_fields()

    # 下载漫画数据
    queue = JoinableQueue()
    counter = Value("i", 0)
    producer = Process(target=task_producer, args=(queue, counter,))
    producer.start()
    consumer_list = []
    for proc in range(max_proc):
        c = Process(target=task_consumer, args=(queue, counter,), daemon=True)
        consumer_list.append(c)
    for c in consumer_list:
        c.start()
    producer.join()
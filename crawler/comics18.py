import math
import os
import re
import shutil
import time
import hashlib
import random

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from PIL import Image
from mongo import mongo

import pprint


def get_image_md5(img_path):
    img = Image.open(img_path)
    img_bytes = img.tobytes()
    md5hash = hashlib.md5(img_bytes)
    img.close()
    return md5hash.hexdigest()


def get_filename_from_url(url):
    if '?' in url:
        tmp_idx = url.index('?')
        tmp_url = url[:tmp_idx]
        start_idx = tmp_url.rindex('/') + 1
        return tmp_url[start_idx:]
    else:
        start_idx = url.rindex('/') + 1
        return url[start_idx:]


def get_slice_size(album_id, pic_index):
    slice_num = 10
    if int(album_id) >= 268850:
        n = f"{album_id}{pic_index}"
        md5_hasher = hashlib.md5()
        md5_hasher.update(n.encode("utf-8"))
        md5_str = md5_hasher.hexdigest()
        tmp = ord(md5_str[-1]) % 10
        if tmp == 0:
            slice_num = 2
        elif tmp == 1:
            slice_num = 4
        elif tmp == 2:
            slice_num = 6
        elif tmp == 3:
            slice_num = 8
        elif tmp == 4:
            slice_num = 10
        elif tmp == 5:
            slice_num = 12
        elif tmp == 6:
            slice_num = 14
        elif tmp == 7:
            slice_num = 16
        elif tmp == 8:
            slice_num = 18
        elif tmp == 9:
            slice_num = 20
    return slice_num


# 重组图片，以正确的方式现实
def reorder_image(album_id, pic_index, img_path, album_url):
    if not os.path.exists(img_path):
        return
    slice_size = get_slice_size(album_id, pic_index)
    try:
        original_img = Image.open(img_path)
    except:
        return
    img_width, img_height = original_img.size
    remain = int(img_height % slice_size)
    m = 0
    reordered_img = Image.new("RGB", (img_width, img_height))
    while m < slice_size:
        slice_height = math.floor(img_height / slice_size)
        g = slice_height * m
        upper = img_height - slice_height * (m + 1) - remain
        if 0 == m:
            slice_height += remain
            bottom = img_height
        else:
            g += remain
            bottom = img_height - (slice_height * m + remain)
        right = img_width
        crop_box = (0, upper, right, bottom)
        try:
            sliced_img = original_img.crop(crop_box)
            paste_box = (0, g)
            reordered_img.paste(sliced_img, paste_box)
        except Exception as e:
            print("crop image exception in url: ", album_url, "with image: ", img_path)
            break
        m += 1
    original_img.close()
    new_img_path = f"{img_path}.original"
    shutil.move(img_path, new_img_path)
    reordered_img.save(img_path)
    reordered_img.close()
    os.remove(new_img_path)


class Comics18(object):
    def __init__(self, config):
        self.config = config
        db_conf = config.data['db']
        self.av91_config = self.config.data["comics18"]
        self.db_inst = mongo.Mongo(db_conf["host"], db_conf["port"], db_conf["db_name"])
        self.coll_name = db_conf["coll"]
        self.ok_coll = db_conf["ok_coll"]
        self.checked = False


    def get_base_info(self):
        comics18_config = self.config.data["comics18"]
        max_timeout = comics18_config["maxReqTimeout"]
        page_tpl = comics18_config["pageTpl"]
        start_page = comics18_config["startPage"]
        max_page = comics18_config["maxPage"]
        domain = comics18_config["domain"]
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False, channel="chrome")
            context = browser.new_context()
            context.set_default_timeout(max_timeout * 1000)
            while start_page <= max_page:
                page_url = page_tpl.format(start_page)
                page = context.new_page()
                page.route(
                    re.compile(r"(\.js$)|(\.css$)|(\.php$)|(\.svg$)|(\.m3u8$)|(\.ts$)|(\.woff$)"),
                    lambda route: route.abort())
                page.goto(page_url,wait_until="domcontentloaded")
                page.evaluate('document.cookie="cover=1"')
                row_list = page.query_selector_all("div#wrapper > div.container > div.row")
                comic_page_item_list = []
                for row in row_list:
                    row.scroll_into_view_if_needed()
                    comic_item_list = row.query_selector_all("div.list-col > div.p-b-15")
                    if len(comic_item_list) > 0:
                        comic_page_item_list.extend(comic_item_list)
                for comic_item in comic_page_item_list:
                    comic_item.scroll_into_view_if_needed()
                    title = comic_item.query_selector("span.video-title").inner_text()
                    cover_elem = comic_item.query_selector("div:nth-child(1)")
                    authors_elem = comic_item.query_selector_all("div.title-truncate.hidden-xs > a")
                    authors = []
                    for author in authors_elem:
                        authors.append(author.inner_text())
                    if cover_elem is not None:
                        album_elem = cover_elem.query_selector("a:nth-child(1)")
                        if album_elem is not None:
                            alum_url = album_elem.get_attribute("href")
                            alum_url = f"{domain}{alum_url}"
                            pic_elem = album_elem.query_selector("img")
                            cover_url = pic_elem.get_attribute("data-original")
                            loveicon_elem = cover_elem.query_selector("div.label-loveicon > a > span")
                            love_cnt = loveicon_elem.inner_text()
                            comic_tags_elem = comic_item.query_selector_all("div.title-truncate.tags > a")
                            comic_tags = []
                            for comic_tag in comic_tags_elem:
                                comic_tags.append(comic_tag.inner_text())
                            category_tags_elem = cover_elem.query_selector_all("div.category-icon > div")
                            category_tags = []
                            for category_tag in category_tags_elem:
                                category_tags.append(category_tag.inner_text())
                            if 'K' in love_cnt:
                                love_cnt = float(str(love_cnt).split('K')[0]) * 1000
                            elif 'M' in love_cnt:
                                love_cnt = float(str(love_cnt).split('M')[0]) * 1000000
                            base_info = {
                                "title": title,
                                "album_url": alum_url,
                                "cover_url": cover_url,
                                "love_cnt": int(love_cnt),
                                "category_tags": category_tags,
                                "authors": authors,
                                "comic_tags": comic_tags,
                                "info_complete": False
                            }
                            doc = self.db_inst.find_one(self.coll_name, {"album_url": alum_url})
                            if doc is None:
                                self.db_inst.insert_one(self.coll_name, base_info)
                            else:
                                self.db_inst.update_one(self.coll_name, {"_id": doc["_id"]}, {"$set": base_info})
                print(start_page, " finished >>>>>>>>>>")
                page.close()
                start_page += 1


    def get_detail_info(self, proc_id):
        comics18_config = self.config.data["comics18"]
        max_timeout = comics18_config["maxReqTimeout"]
        domain = comics18_config["domain"]
        page_size = 321
        skip = (proc_id - 1) * page_size
        comics_cursor = self.db_inst.find_all(self.coll_name,
                                                  {},
                                                  sort_key="love_cnt",
                                                  sort_val=-1).limit(2).skip(3)
        comics_list = [item for item in comics_cursor]
        for comic_item in comics_list:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False, channel="chrome")
                context = browser.new_context()
                context.set_default_timeout(max_timeout * 1000)
                doc_id = comic_item["_id"]
                album_url = comic_item["album_url"]
                page = context.new_page()
                # page.route(
                #     re.compile(r"(\.gif$)|(\.png$)|(\.jpg$)|(\.css$)|(\.php$)|(\.svg$)|(\.m3u8$)|(\.ts$)|(\.woff$)"),
                #     lambda route: route.abort())
                try:
                    result = False
                    while result != True:
                        try:
                            page = context.new_page()
                            page.goto(album_url,timeout = 5000,wait_until="domcontentloaded")
                            result = True
                        except PlaywrightTimeoutError:
                            page.close()
                            result = False
                    album_cover = page.query_selector("#album_photo_cover > div.thumb-overlay > a")
                    album_read_url = album_cover.get_attribute("href")
                    album_cover_img_url = album_cover.query_selector("img").get_attribute("src")
                    basic_info = page.query_selector(
                        "#wrapper > div.container > div:nth-child(4) > div > div.panel.panel-default.visible-lg.hidden-xs > div.panel-body > div > div.col-lg-7 > div")
                    jmid = basic_info.query_selector("div:nth-child(1)").inner_text()
                    page_text = basic_info.query_selector("div:nth-child(9)").inner_text()
                    publish_date = basic_info.query_selector("div:nth-child(10) > span:nth-child(1)").get_attribute(
                        "content")
                    update_date = basic_info.query_selector("div:nth-child(10) > span:nth-child(2)").get_attribute(
                        "content")
                    view_count = basic_info.query_selector("div:nth-child(10) > span:nth-child(3)").inner_text()
                    click_count = basic_info.query_selector("div:nth-child(10) > span:nth-child(4)").inner_text()
                    img_zoom_list = page.query_selector_all("div.img_zoom > div.img_zoom_img > img")
                    zoom_img_list = []
                    for img_zoom in img_zoom_list:
                        img_zoom_src = img_zoom.get_attribute("data-original")
                        zoom_img_list.append(img_zoom_src)
                    intro_block = page.query_selector("div#intro-block")
                    comic_desc = intro_block.query_selector("div:nth-child(1)").inner_text()
                    works = intro_block.query_selector_all("div:nth-child(2) > span > a")
                    work_list = []
                    for work in works:
                        work_list.append(work.inner_text())
                    actors = intro_block.query_selector_all("div:nth-child(3) > span > a")
                    actor_list = []
                    for actor in actors:
                        actor_list.append(actor.inner_text())
                    labels = intro_block.query_selector_all("div:nth-child(4) > span > a")
                    label_list = []
                    for label in labels:
                        label_list.append(label.inner_text())
                    authors = intro_block.query_selector_all("div:nth-child(5) > span > a")
                    author_list = []
                    for author in authors:
                        author_list.append(author.inner_text())
                    episode_block = page.query_selector("div#episode-block > div > div.episode")
                    episode_list = []
                    if episode_block is not None:
                        episode_elem_list = episode_block.query_selector_all("a")
                        for episode_elem in episode_elem_list:
                            episode_src = episode_elem.get_attribute("href")
                            episode_src = f"{domain}{episode_src}"
                            episode_text = episode_elem.query_selector("li").inner_text()
                            episode_info = episode_text.split("\n")
                            info_len = len(episode_info)
                            if episode_info[info_len - 1].strip() == "":
                                update_date = episode_info[info_len - 2].strip()
                            else:
                                update_date = episode_info[info_len - 1].strip()
                            print("update date: ", update_date)
                            episode_list.append({
                                "index": episode_info[1],
                                "update_date": update_date,
                                "episode_url": episode_src
                            })

                    obj = {
                        "album_read_url": album_read_url,
                        "album_cover_url": album_cover_img_url,
                        "jmid": jmid.replace("\n", "").replace("禁漫車：", "").strip(),
                        "page_text": page_text.replace("\n", "").replace("頁數：", "").strip(),
                        "publish_date": publish_date,
                        "update_date": update_date,
                        "view_count": view_count.replace("\n", "").replace("次觀看", "").strip(),
                        "click_count": click_count.replace("\n", "").replace("點擊喜歡", "").strip(),
                        "zoom_img_list": zoom_img_list,
                        "comic_desc": comic_desc,
                        "work_list": work_list,
                        "actor_list": actor_list,
                        "label_list": label_list,
                        "author_list": author_list,
                        "episode_list": episode_list,
                        "info_complete": True
                    }
                    self.db_inst.update_one(self.coll_name, {"_id": doc_id}, {"$set": obj})
                    page.close()
                except Exception as e:
                    if page.url == " https://18comic.vip/error/album_missing":
                        self.db_inst.del_docs(self.coll_name, {"_id": doc_id})
                    print("exception in album url:", album_url, page.url)
                    print(e)
                    page.close()

    def produce_comic_download_task(self, joinable_queue, mp_counter):
        comics_config = self.config.data["comics18"]
        max_timeout = comics_config["maxReqTimeout"]
        domain = comics_config["domain"]
        max_proc = comics_config["maxDownloadProc"]
        save_base_path = comics_config["saveBasePath"]
        page_size = 13710
        page_num = 10
        start_page = 1
        reorder_const = 220980
        while start_page <= page_num:
            skip = (start_page - 1) * page_size
            comics_cursor = self.db_inst.find_all(self.coll_name,
                                                  {},
                                                  sort_key="love_cnt",
                                                  sort_val=-1).limit(2).skip(3)
            comics_list = [item for item in comics_cursor]
            for comic_item in comics_list:
                comic_album_url = comic_item["album_url"]
                read_url = comic_item["album_read_url"]
                album_id = read_url.split("/")[2]
                cover_url = comic_item["album_cover_url"]
                title = comic_item["title"].replace("/", "-")
                # upload_priority = comic_item["upload_priority"]
                doc_id = comic_item["_id"]
                comic_save_base_path = f"{save_base_path}/{title}"
                episode_list = comic_item["episode_list"]
                if len(episode_list) == 0:
                    page_url = f"{domain}{read_url}"
                    item_path = f"{save_base_path}/{title}"
                    if self.db_inst.count_docs(self.ok_coll, {"album_url": page_url}) > 0:
                        if os.path.exists(item_path.encode("utf-8")):
                            print(page_url, " already downloaded")
                            continue
                        else:
                            self.db_inst.del_docs(self.ok_coll, {"album_url": page_url})
                    try:
                        print("start going to: ", title, " : ", page_url)
                        with sync_playwright() as p:
                            browser = p.chromium.launch(headless=False, channel="chrome")
                            context = browser.new_context(viewport={
                                "width": 1920,
                                "height": 1200
                            })
                            context.set_default_timeout(120 * 1000)
                            page = context.new_page()
                            result = False
                            while result != True:
                                try:
                                    page = context.new_page()
                                    page.goto(page_url,timeout = 5000,wait_until="domcontentloaded")
                                    result = True
                                except PlaywrightTimeoutError:
                                    page.close()
                                    result = False
                                except Exception as e:
                                    result = True
                                    print(e)
                            # page.wait_for_load_state("networkidle")
                            # cover_check = page.wait_for_selector("button#chk_cover")
                            # cover_check.click()
                            # guide_check = page.wait_for_selector("button#chk_guide")
                            # guide_check.click()
                            page.wait_for_selector("div.scramble-page")
                            scramble_list = page.query_selector_all("div.scramble-page")
                            if page_url[-1] == "/":
                                page_url = page_url[:-1]
                            album_id = page_url[page_url.rindex("/") + 1:]
                            album_id = int(album_id)
                            if album_id < reorder_const:
                                need_reorder = False
                            else:
                                need_reorder = True
                            img_list = []
                            for scramble_item in scramble_list:
                                # scramble_item.scroll_into_view_if_needed()
                                item_id = scramble_item.get_attribute("id").split(".")[0]
                                img_src = scramble_item.query_selector("img").get_attribute("data-original")
                                img_list.append({
                                    "item_id": item_id,
                                    "img_url": img_src,
                                    "need_reorder": need_reorder,
                                })
                            joinable_queue.put({
                                "doc_id": doc_id,
                                "album_id": album_id,
                                "cover_url": cover_url,
                                "title": title,
                                "img_list": img_list,
                                "album_url": page_url,
                                "base_save_path": comic_save_base_path,
                                "save_path": comic_save_base_path,
                                "serialize": False,
                            })
                            page.close()
                            with mp_counter.get_lock():
                                mp_counter.value += 1
                            while mp_counter.value >= max_proc:
                                time.sleep(1)
                    except Exception as e:
                        if page.url == "https://18comic.vip/error/album_missing":
                            self.db_inst.del_docs(self.coll_name, {"_id": doc_id})
                        print("exception in url: ", comic_album_url, " with : ", e)
                else:
                    for episode_item in episode_list:
                        idx = episode_item["index"].strip()
                        episode_url = episode_item["episode_url"]
                        if episode_url[-1] == "/":
                            episode_url = episode_url[:-1]
                        album_id = episode_url[episode_url.rindex("/") + 1:]
                        album_id = int(album_id)
                        # album_id = episode_url.split("/")[-1]
                        episode_save_path = f"{comic_save_base_path}/{idx}"
                        item_path = f"{save_base_path}/{title}/{idx}"
                        if self.db_inst.count_docs(self.ok_coll, {"album_url": episode_url}) > 0:
                            if os.path.exists(item_path.encode("utf-8")):
                                print(title, ":", idx, " already downloaded")
                                continue
                            else:
                                self.db_inst.del_docs(self.ok_coll, {"album_url": episode_url})
                        try:
                            print("start going to: ", title, " : ", episode_url)
                            with sync_playwright() as p:
                                browser = p.chromium.launch(headless=False, channel="chrome")
                                context = browser.new_context(viewport={
                                    "width": 1920,
                                    "height": 1200
                                })
                                context.set_default_timeout(120 * 1000)
                                page = context.new_page()
                                result = False
                                while result != True:
                                    try:
                                        page = context.new_page()
                                        page.goto(episode_url,timeout = 5000,wait_until="domcontentloaded")
                                        result = True
                                    except PlaywrightTimeoutError:
                                        page.close()
                                        result = False
                                # page.wait_for_load_state("networkidle")
                                # cover_check = page.wait_for_selector("button#chk_cover")
                                # cover_check.click()
                                # guide_check = page.wait_for_selector("button#chk_guide")
                                # guide_check.click()
                                page.wait_for_selector("div.scramble-page")
                                scramble_list = page.query_selector_all("div.scramble-page")
                                img_list = []
                                if album_id < reorder_const:
                                    need_reorder = False
                                else:
                                    need_reorder = True
                                for scramble_item in scramble_list:
                                    # scramble_item.scroll_into_view_if_needed()
                                    item_id = scramble_item.get_attribute("id").split(".")[0]
                                    img_src = scramble_item.query_selector("img").get_attribute("data-original")
                                    img_list.append({
                                        "item_id": item_id,
                                        "img_url": img_src,
                                        "need_reorder": need_reorder
                                    })
                                joinable_queue.put({
                                    "doc_id": doc_id,
                                    "album_id": album_id,
                                    "cover_url": cover_url,
                                    "title": title,
                                    "img_list": img_list,
                                    "album_url": episode_url,
                                    "base_save_path": comic_save_base_path,
                                    "save_path": episode_save_path,
                                    "serialize": True,
                                })
                                with mp_counter.get_lock():
                                    mp_counter.value += 1
                                while mp_counter.value >= max_proc:
                                    time.sleep(1)
                        except Exception as e:
                            if page.url == "https://18comic.vip/error/album_missing":
                                self.db_inst.del_docs(self.coll_name, {"_id": doc_id})
                            print("exception in url: ", comic_album_url, " with : ", e)
            browser.close()
            start_page += 1

    def consume_comics_download_task(self, download_item):
        #pprint.pprint(download_item)
        doc_id = download_item["doc_id"]
        album_id = download_item["album_id"]
        cover_url = download_item["cover_url"]
        title = download_item["title"]
        img_list = download_item["img_list"]
        base_save_path = download_item["base_save_path"]
        save_path = download_item["save_path"]
        album_url = download_item["album_url"]
        serialize = download_item["serialize"]
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False, channel="chrome")
            context = browser.new_context()
            context.set_default_timeout(300 * 1000)
            page = context.new_page()
            self.download_file(cover_url, base_save_path,page, file_name=f"{title}.jpg")
        if len(img_list) <= 0:
            return
        download_counter = 0
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False, channel="chrome")
            context = browser.new_context()
            context.set_default_timeout(300 * 1000)
            page = context.new_page()
            for img_item in img_list:
                img_url = img_item["img_url"]
                item_id = img_item["item_id"]
                need_reorder = img_item["need_reorder"]
                img_path = f"{save_path}/{item_id}.jpg"
                retry_cnt = 0
                state = False
                while not state and retry_cnt < 3:
                    state = self.download_file(img_url, save_path,page)
                    time.sleep(3)
                    retry_cnt += 1
                if state:
                    download_counter += 1
                    if need_reorder:
                        print("重組中...")
                        reorder_image(album_id, item_id, img_path, album_url)
                else:
                    return
        if download_counter < len(img_list):
            return
        else:
            self.db_inst.insert_one(self.ok_coll, {"doc_id": doc_id, "album_url": album_url,
                                                   "title": title, "serialize": serialize, "chapter_uploaded": False})

    def download_file(self, file_url, file_save_path, page,file_name="", headers=None):
        headers = {
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
                    "Accept":"*/*",
                    "Accept-Encoding":"gzip, deflate, br",
                    "Connection":"keep-alive"
        }
        if not os.path.exists(file_save_path.encode('utf-8')):
            os.makedirs(file_save_path)
        if file_name == "":
            file_name = get_filename_from_url(file_url)
        file_path = "{}/{}".format(file_save_path, file_name)
        file_path_tmp = "{}/{}.tmp".format(file_save_path, file_name)
        download_status = False
        if os.path.exists(file_path.encode('utf-8')):
            os.remove(file_path)
        comics_config = self.config.data["comics18"]
        timeout = comics_config["maxReqTimeout"]
        try:
            print("下載")
            result = False
            while result != True:
                try:
                    rs = page.goto(file_url,timeout = 3000,wait_until="domcontentloaded")
                    result = True
                except PlaywrightTimeoutError:
                    page.close()
                    result = False
        # rs = requests.session()
        # resp = rs.get(file_url, headers=headers, stream=True, timeout=30)
            if rs.status != 200:
                print(rs.status, ":", file_url)
                return download_status
            else:
                print(rs.status, ":", file_url)
                with open(file_path_tmp, "wb") as f:
                    # for chunk in rs.request.iter_content(chunk_size=1024 * 1024 * 4):
                        # print(chunk)
                        # if chunk:
                        #     f.write(chunk)
                    f.write(rs.body())
                shutil.move(file_path_tmp, file_path)
                if ".webp" in file_url:
                    img = Image.open(file_path).convert("RGB")
                    new_file_path = file_path.replace(".webp", ".jpg")
                    img.save(new_file_path, "jpeg")
                    os.remove(file_path)
                download_status = True
        except requests.exceptions.RequestException as e:
            download_status = False
            print(e)
        return download_status

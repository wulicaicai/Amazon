import random
import string
import pymysql
import warnings
import requests, re, json, time, datetime, multiprocessing
from lxml import etree
from user_agent import generate_user_agent
from urllib import parse

warnings.filterwarnings('ignore')
count_ = 0
limitStart = 0


# 时间戳处理
def returnTime(timeStr):
    print('returnTime')
    if timeStr == 0:
        return 0
    try:
        review_time_stamp = int(time.mktime(time.strptime(timeStr, '%Y-%m-%d')))
    except:
        try:
            timeStr = timeStr.replace('-Sept-', '-Sep-')
            review_time_stamp = int(time.mktime(time.strptime(timeStr, '%Y-%B-%d')))
        except:
            review_time_stamp = int(time.mktime(time.strptime(timeStr, '%Y-%b-%d')))
    finally:
        return review_time_stamp


# 解析日期 signal = 区分table 和 ul 两种抓取 和 计算分分数, 1 为table 2
def dateTime(dateandtime, signal):
    print('dateTime')
    month = ['Jan', 'Feb', 'Mar', 'April', 'May', 'Jun', 'July', 'Aug', 'Sept', 'Oct',
             'Nov', 'Dec']
    month_re = re.compile(r'\/(\d+)\/')
    month_num = int(month_re.findall(dateandtime)[0])
    # month_num = month.index(DateFirstAvailableMonth)
    # month_num += 1
    # 抓取日期 转为整数
    if signal == 1:
        date_re = re.compile(r'\d{4}\/\d+\/(\d+)')
        date_num = int(date_re.findall(dateandtime)[0])
    elif signal == 2:
        date_re = re.compile(r'\d{4}\/\d+\/(\d+)')
        date_num = int(date_re.findall(dateandtime)[0])
    # 抓取年份 转为整数
    year_re = re.compile(r'(\d{4}).*?')
    year_num = int(year_re.findall(dateandtime)[0])
    DateFirstAvailable = [year_num, month_num, date_num]
    datec = datetime.datetime(year_num, month_num, date_num)
    returnTimes = time.mktime(datec.timetuple())

    return returnTimes, DateFirstAvailable


# 遍历回调请求链接
def getHtmlCallbak(url, s=None, sendTime=0):
    print('getHtmlCallbak')
    urlstatue = 0
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'en-US,en;q=0.8',
               'Cache-Control': 'max-age=0',
               'User-Agent': generate_user_agent(device_type="desktop"),
               'Connection': 'keep-alive',
               'upgrade-insecure-requests': '1',
               ''.join(random.sample(string.ascii_lowercase, random.randint(3, 5))): ''.join(
                   random.sample(string.ascii_lowercase, random.randint(3, 5)))
               }
    try:
        proxy = json.loads(
            requests.get('http://third.gets.com/api/index.php?act=getProxyIp&sec=20171212getscn',timeout=45).text)
        proxy = {
            'http': 'http://%s:%s' % (proxy['data'][0]['ip'], proxy['data'][0]['port']),
            'https': 'http://%s:%s' % (proxy['data'][0]['ip'], proxy['data'][0]['port'])
        }
        print(proxy)
        req = s.get(url, headers=headers, timeout=10, proxies=proxy)
        # req = s.get(url, headers=headers, timeout=10)
        urlstatue = req.status_code
        req.raise_for_status()  # 如果状态不是200，则引发异常
        html = req.text
        time.sleep(1)
    except Exception as e:
        print(e)
        if sendTime > 15:
            print(u'more than 15 time ' + url)
            return '404'
        if urlstatue == 404:
            print(u'return 404')
            return '404'
        sendTime += 1
        return getHtmlCallbak(url, s, sendTime)
    return html


#  评论分数处理        连接    产品id      页数       列表
def getOneWeekReview(urlReview, ASIN, reviewCount, review_date):
    print('getOneWeekReview')
    now = int(time.time())
    review_time = ''
    review_time_stamp = 0
    last30day = now - 30 * 24 * 3600
    print(urlReview)
    s = requests.session()
    reviewsHtml = getHtmlCallbak(urlReview, s)
    # html = etree.HTML(reviewsHtml)
    # TimeList = html.xpath('//div[@data-hook="review"]//span[@data-hook="review-date"]/text()')
    reEs = re.findall(
        r'<span data-hook="review-date" class="a-size-base a-color-secondary review-date">([^<]*)<\/span>',
        reviewsHtml)
    if reEs != []:
        for ret in reEs:
            review_time = ret.replace('年', '-').replace('月', '-').replace('日', '').strip()
            review_time_stamp = returnTime(review_time)
            if review_time_stamp >= last30day:
                review_date += [review_time_stamp]
            else:
                review_date += [review_time_stamp]
                break
        callReviewDate = review_date[-1]  # 最后一个时间戳还是再30天内 就继续爬取第二页,如果没有就
        reviewCount += 1;
        if callReviewDate >= last30day and len(reEs) == 10:
            url = 'https://' + parse.urlparse(
                urlReview).netloc + '/product-reviews/' + ASIN + '/ref=cm_cr_getr_d_show_all?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=' + str(
                reviewCount)
            return getOneWeekReview(url, ASIN, reviewCount, review_date)
        else:
            review_date_num = len(review_date) if callReviewDate >= last30day else len(review_date) - 1
            score = 50 if review_date_num >= 25 else review_date_num * 2;
            return score
    else:
        score = 0
        return score


# 回复分数处理
def getOneWeekQuestion(urlAnswered, ASIN, answerCount, questionDate):
    print('getOneWeekQuestion')
    review_time = ''
    review_time_stamp = 0
    now = int(time.time())
    questionDate = questionDate
    last30day = now - 30 * 24 * 3600
    print(urlAnswered)
    s = requests.session()
    reviewsHtml = getHtmlCallbak(urlAnswered, s)
    reEs = re.findall(r'<span class="a-color-tertiary aok-align-center">[^<]*&#183([^<]*)<\/span>', reviewsHtml)
    if reEs != []:
        for ret in reEs:
            question_time = ret.strip().replace(',', '').split(" ")
            question_time = question_time[2] + '-' + question_time[0] + '-' + (
                '0' + question_time[1] if int(question_time[1]) < 10 else question_time[1])
            question_time_stamp = returnTime(question_time)

            if question_time_stamp >= last30day:
                questionDate += [question_time_stamp]
            else:
                questionDate += [question_time_stamp]
                break
        callQuestionDate = questionDate[-1]
        answerCount += 1
        if callQuestionDate >= last30day and len(reEs) == 10:
            url = 'https://' + parse.urlparse(urlAnswered).netloc + '/ask/questions/asin/' + ASIN + '/' + str(
                answerCount) + '/?sort=SUBMIT_DATE&isAnswered=true'
            return getOneWeekQuestion(url, ASIN, answerCount, questionDate)
        else:
            questionDate_num = len(questionDate) if callQuestionDate >= last30day else len(questionDate) - 1
            score = 50 if questionDate_num >= 25 else questionDate_num * 2;
            return score
    else:
        score = 0
        return score


def GET_API():
    global limitStart
    db = pymysql.connect(host='221.5.103.7', user='collection_test', password='Milie@#121', database='collection',charset='utf8', port=3306)
    cursor = db.cursor()
    # 利用游标对象的execute()方法执行
    setdb = 'select url from `amazon_new_releases` where `site` = 8 and main_sort in (1,3,8,13,19,28) or id in (1,3,8,13,19,28) limit %s ,10' % limitStart
    cursor.execute(setdb)
    tupl = cursor.fetchall()
    if tupl:
        limitStart += 10
    else:
        limitStart = 0
    urltupls = list(tupl)
    # 提交到数据库执行
    db.commit()
    # 关闭游标
    cursor.close()
    # 关闭对象
    return urltupls


def parse_():
    print('parse_')
    start_urls = GET_API()
    fullUrlList = []
    url = 'https://www.amazon.jp'
    for start_url in start_urls:
        # 因为识别机器人，所以用 getHtmlCallbak 去获取数据
        start_url = list(start_url)[0]
        s = requests.session()
        info = getHtmlCallbak(start_url, s)
        sonXpath = etree.HTML(info)
        if sonXpath.xpath('//div[@id="zg_left_col1"]'):
            sonUrl = sonXpath.xpath('//div[@class="a-fixed-left-grid-col a-col-left"]/a/@href')
            for sonurl in sonUrl:
                fullUrl = url + sonurl
                fullUrlList.append(fullUrl)
    infoParse(fullUrlList)


def infoParse(fullUrlList):
# def infoParse():
    for fullUrl in fullUrlList:
    # for fullUrl in [
    #     'https://www.amazon.co.jp/%E3%83%8F%E3%83%BC%E3%83%8D%E3%82%B9%E7%94%A8%E8%85%BF%E3%83%99%E3%83%AB%E3%83%88%E3%83%8F%E3%83%B3%E3%82%AC%E3%83%BC-MHG-%E8%90%BD%E4%B8%8B%E9%98%B2%E6%AD%A2-%E9%9B%BB%E6%B0%97%E5%B7%A5%E4%BA%8B-%E9%AB%98%E6%89%80%E3%81%A7%E3%81%AE%E5%AE%89%E5%85%A8%E4%BD%9C%E6%A5%AD/dp/B0143VDONQ/ref=lp_2039654051_1_26?s=industrial&ie=UTF8&qid=1554983623&sr=1-26',
    #     'https://www.amazon.jp/%E3%83%90%E3%83%9C%E3%83%A9-%E3%83%90%E3%83%89%E3%83%9F%E3%83%B3%E3%83%88%E3%83%B3-%E3%82%B7%E3%83%A3%E3%83%89%E3%82%A6%E3%83%84%E3%82%A2%E3%83%BC-%E6%97%A5%E6%9C%AC%E3%83%90%E3%83%89%E3%83%9F%E3%83%B3%E3%83%88%E3%83%B3%E5%8D%94%E4%BC%9A%E6%A4%9C%E5%AE%9A%E5%AF%A9%E6%9F%BB%E5%90%88%E6%A0%BC%E5%93%81-BASF1902/dp/B07NGF7KHX/ref=zg_bsnr_2221115051_2/358-9353012-5693438?_encoding=UTF8&refRID=VK8G3T5C1D6VWHP4DE03']:
# 抓取 Asin 产品标志
        ASIN_re = re.compile(r'dp/(.*?)/ref')
        ASIN = ASIN_re.findall(fullUrl)
        if ASIN:
            ASIN = ASIN[0]
        else:
            ASIN_re = re.compile(r'dp/(.*?)\?ref')
            ASIN = ASIN_re.findall(fullUrl)[0]

        host = parse.urlparse(fullUrl).scheme + '://' + parse.urlparse(fullUrl).netloc + '/'
        if '&url=' in fullUrl:  # 判断是否有"&url=" 字符串
            proUrlStr = fullUrl.split('&url=')  # 根据url=进行切片
            fullUrl = parse.unquote(proUrlStr[1])  # 切片取值
        fullUrl = fullUrl.split('/ref')[0] + '?psc=1'
        fullUrl = '/dp' + fullUrl.split('/dp')[1]
        fullUrl = host+fullUrl
        print('infoParse', fullUrl)

        s = requests.session()
        try:
            info = getHtmlCallbak(fullUrl, s)
        except:
            print('在infoParse中报错 getHtmlCallbak')
            info = getHtmlCallbak(fullUrl, s)

        infoXpath = etree.HTML(info)


        # 星级
        if infoXpath.xpath(
                '//span[@class="a-declarative"]/a[1]/i[@class="a-icon a-icon-star a-star-4"]/span[@class="a-icon-alt"]'):
            stars = infoXpath.xpath(
                '//span[@class="a-declarative"]/a[1]/i[@class="a-icon a-icon-star a-star-4"]/span[@class="a-icon-alt"]/text()')[
                0]
            stars_re = re.compile(r'(\d\.\d)')
            stars_list = stars_re.findall(stars)
            stars = float(stars_list[0])  # 转为浮点型 好做计算用
        else:
            stars = 0
        # 抓取产品名称
        try:
            title = infoXpath.xpath('//span[@id="productTitle"]/text()')[0].replace('\n', '').strip()
        except:
            continue
        # 抓取产品图片
        images = []
        imageR = re.search(r"\'initial\': (.*)},(.*)\'colorToAsin\'", info, re.S)  # 获取图片 用以前的老方法
        if imageR:
            imageRes = json.loads(imageR.group(1))
            for img in imageRes:
                if img['hiRes']:
                    imgSrc = re.sub(re.search(r".*\/[^\.]*(\..*)\.jpg", img['hiRes']).group(1), '',
                                    img['hiRes'])
                    images += [imgSrc]
                else:
                    images += [img['large']]
        # 商店名字
        if infoXpath.xpath('//a[@id="bylineInfo"]/text()'):
            shop = infoXpath.xpath('//a[@id="bylineInfo"]/text()')[0].replace('/n', '').strip()
            if 'by' in shop:
                shop = 'by' + ' ' + shop[2:len(shop)]
            else:
                shop = 'by' + ' ' + shop
        else:
            shop = ' '
            # 评论数 reviews
        if infoXpath.xpath(
                '//div[@class="feature"]/div[@id="averageCustomerReviews"]/span[@class="a-declarative"]/a[@id="acrCustomerReviewLink"]/span[@id="acrCustomerReviewText"]/text()'):
            reviews = infoXpath.xpath(
                '//span[@id="acrCustomerReviewText"]/text()')
            reviews_re = re.compile('(\d+).*?')
            reviews = reviews_re.findall(reviews[0])
            reviews = int(reviews[0])
        else:
            reviews = ' '
        # 回复数
        if infoXpath.xpath('//span[@class="celwidget"]/a/span[@class="a-size-base"]'):  # 获取回复数
            answered = infoXpath.xpath('//span[@class="celwidget"]/a/span[@class="a-size-base"]/text()')[0].strip()
            answered_re = re.compile(r'\d+')
            answered = answered_re.findall(answered)
            answered = int(answered[0])
        else:
            answered = ' '

        # 介绍 五要素
        if infoXpath.xpath(
                '//div[@id="feature-bullets"]/ul[@class="a-unordered-list a-vertical a-spacing-none"]/li[not(@id)]/span'):  # 介绍
            bulletsList = []
            bullet = infoXpath.xpath(
                '//div[@id="feature-bullets"]/ul[@class="a-unordered-list a-vertical a-spacing-none"]/li[not(@id)]/span/text()')
            for b in bullet:
                bulletsList.append(b.replace('\n', '').replace('\t', '').replace(' ', ''))
        else:
            bullets = ' '
        # 价格
        Price_re = re.compile(
            r'<span id="priceblock_ourprice" class="a-size-medium a-color-price priceBlockBuyingPriceString">(.*?)</span>')
        Price = Price_re.findall(info)
        if Price == []:
            Price = ' '
        else:
            Price = Price[0]
            if Price.find('-') != -1:
                Price_lre=re.compile(r'(.*?)\-.*?')
                Price = Price_lre.findall(Price)[0]
        ListPrice_re = re.compile(r'<span class="priceBlockStrikePriceString a-text-strike"> (.*?)</span>')
        ListPrice = ListPrice_re.findall(info)
        if ListPrice == []:
            ListPrice = Price
        else:
            ListPrice = ListPrice[0]
        # if infoXpath.xpath('//div[@id="price"]/table/tr[1]/td[1]/text()') in ['List Price:', 'Was:']:
        #     ListPrice = infoXpath.xpath('//div[@id="price"]/table/tr[1]/td[2]/span[1]/text()')[0]
        #     Price = infoXpath.xpath('//span[@id="priceblock_ourprice"]/text()')[0]
        # elif infoXpath.xpath('//span[@id="priceblock_dealprice"]/text()'):
        #     Price = infoXpath.xpath('//span[@id="priceblock_ourprice"]/text()')[0]
        #     ListPrice = Price
        # # 因为标签不一样,所以多出此方法进行处理
        # elif infoXpath.xpath('//span[@id="priceblock_ourprice"]/div[@class="a-section a-spacing-none"]'):
        #     m = infoXpath.xpath('//span[@id="priceblock_ourprice"]/div[@class="a-section a-spacing-none"]/text()')[0]
        #     m = m.strip()
        #     n = infoXpath.xpath(
        #         '//span[@id="priceblock_ourprice"]/div[@class="a-section a-spacing-none"]/span[@class="a-size-small price-info-superscript"][2]/text()')[
        #         0]
        #     n = n.strip()
        #     ListPrice = '£' + m + '.' + n
        #     print(ListPrice)
        # else:
        #     Price = ' '
        #     ListPrice = ' '
        keywords = ' '  # 关键字抓取
        keywords_re = re.compile(r'<meta name\=\"keywords\" content\=\"(.*?)\" \/\>',re.S)
        keywords = keywords_re.findall(info)[0]

        # 产品描述
        if infoXpath.xpath('//div[@class="feature"]/li//text()'):
            ProductdescriptionList = []
            Productdescriptions = infoXpath.xpath('//div[@class="feature"]/li//text()')
            for Productdescription in Productdescriptions:
                Productdescription.replace('\n', '').replace('\t', '').strip()
                ProductdescriptionList.append(Productdescription)
            Productdescription = ','.join(ProductdescriptionList)
            Productdescription = Productdescription.replace('\n', '').replace(' ', '').strip()
        else:
            if infoXpath.xpath('//meta[@name="description"]/@content'):
                Productdescription = infoXpath.xpath('//meta[@name="description"]/@content')[0].replace('\n',
                                                                                                        '').replace(
                    '\t', '').strip()
            else:
                Productdescription = ' '
        # 跟卖数
        seller = 0
        seller_re = re.compile(
            r'<a href=".*?">新品の出品：(.*?)</a>')
        sellerlist = seller_re.findall(info)
        if sellerlist == []:
            seller += 0
        else:
            seller += int(sellerlist[0])

        ItemWeight = ' '
        ShippingWeight = ' '
        Best = []
        Best1 = []
        DateFirstAvailable = ' '

        # 表格table抓取

        times = None
        tba = infoXpath.xpath(
            '//div[@class="pdTab"]/table[@cellspacing="0"]/tbody/tr')
        if tba:
            for tr in tba:
                Name = tr.xpath('./td[1]/text()')[0].replace('\n', '').replace(' ', '')
                if Name == '商品重量':  # 产品重量
                    ItemWeight = tr.xpath('./td[2]/text()')[0]
                else:
                    ItemWeight = ' '
                if Name == '発送重量':  # 包装重量
                    ShippingWeight = tr.xpath('./td[2]/text()')[0]
                if Name == 'Amazon.co.jpでの取り扱い開始日':  # 上架时间
                    # 抓取上架时间
                    DateFirstAvailable = tr.xpath('./td[2]/text()')
                    DateFirstAvailable = DateFirstAvailable[0].replace('\n', '').replace(' ', '')
                    times, DateFirstAvailable = dateTime(DateFirstAvailable, 1)

                # 排名等信息
            # span_one = re.compile(r'Amazon Bestsellers Rank:</b>\s+.*?\(<a href="(https:.*?)">.*?</a>\)')
            # span_two = re.compile(
            #     r'<span class="zg_hrsr_rank">(.*?)</span>\s+<span class="zg_hrsr_ladder">(.*?)&nbsp;<a href="(.*?)">(.*?)</a></span>')
            # span_1 = span_one.findall(info)
            # if not span_1:
            #     print('排名等处理空数据')
            # else:
            #     Best1.append(span_1)
            # span_2 = span_two.findall(info)
            # if span_2 == []:
            #     print('排名等处理空数据')
            # else:
            #     for sp in span_2:
            #         Best.append(list(sp))
        # 无表抓取
        if infoXpath.xpath('//div[@id="detail_bullets_id"]'):  # 验证一下table 里有ul 这个class吗
            # 上架时间
            DateFirstAvailable_re = re.compile(r"<li><b> Amazon.co.jp での取り扱い開始日:</b> (.*?)</li>")
            DateFirstAvailable = DateFirstAvailable_re.findall(info)[0]
            if DateFirstAvailable:
                times, DateFirstAvailable = dateTime(DateFirstAvailable, 2)
            else:
                times = None
            # 包装重量
            ShippingWeight_re = re.compile(r'<li><b>発送重量:</b> (.*?)</li>')
            ShippingWeight = ShippingWeight_re.findall(info)
            if ShippingWeight:
                ShippingWeight = ShippingWeight[0]

            # 产品重量
            ItemWeight_re = re.compile(r'<li><b>\s+商品重量:\s+</b>\s+(.*?)\s+</li>')
            ItemWeight = ItemWeight_re.findall(info)
            if ItemWeight:
                ItemWeight = ItemWeight[0]
        # 排名等处理
        span_iurl=re.compile(r'<span class="zg_hrsr_rank">(.*?)</span>\s+<span class="zg_hrsr_ladder">(.*?)&nbsp;<a href="(.*?)">(.*?)</a></span>')
        span1 = span_iurl.findall(info)
        for mes in span1:
            Best1List=[]
            Best.append(list(mes))
            Best1List.append(list(mes)[2])
            Best1.append(Best1List)
        if Best == []:
            Best1List = []
            span_url = re.compile(r'Amazon 売れ筋ランキング:</b>\s+(.*?)\(<a href="(https:.*?)">.*?</a>\)')
            span = span_url.findall(info)
            Best.append(list(span))
            Best1List.append(list(span)[0][1])
            Best1.append(Best1List)
            print('1112')


            # 产品分数
            # 30天内评论数，每个+2分，最高50分
            # 30天内问题数, 每个+2分，最高10分
            # REVIEW 低于2星-20分; 低于3星-10分;3星 0分;4星+10分; 5星+20分;
            # 所属小类目150名以内  每个+10分 最高30分
            # 有Amazon's Choice标志  +20分
            # 产品上架时间为 30天内 +20分；半年内 +15分；一年内 +10分
        score = 0  # 分数

        # 星级分数
        if stars > 0:
            if stars <= 2.7:
                score = -20
            elif stars > 2.7 and stars <= 3.7:
                score = -10
            elif stars > 3.7 and stars <= 4.7:
                score = 10
            elif stars > 4.7:
                score = 20
        # 排名加分
        if len(Best) > 0:
            rankNum = len(Best)
            score += 10 * rankNum

        # Amazon's Choice标志加分
        choice = infoXpath.xpath("//span[@class='ac-badge-text-primary']/text()")
        if choice:
            score += 20
        # 产品上线时间 加分项
        if times:
            if int(times) > time.time() - 2592000:
                score += 20
            elif int(times) > time.time() - 15552000:
                score += 15
            elif int(times) > time.time() - 31104000:
                score += 10

        # 获取评论 分数
        if reviews != ' ':
            if reviews > 0:
                reviewCount = 1
                review_date = []
                urlReview = 'https://' + parse.urlparse(
                    fullUrl).netloc + '/product-reviews/' + ASIN + '/ref=cm_cr_getr_d_show_all?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber=' + str(
                    reviewCount)
                reviewScore = getOneWeekReview(urlReview, ASIN, reviewCount, review_date)
                score += reviewScore

        # 获取回答 分数
        if answered != ' ':
            if answered > 0:
                answerCount = 1
                questionDate = []
                urlAnswered = 'https://' + parse.urlparse(
                    fullUrl).netloc + '/ask/questions/asin/' + ASIN + '/' + str(
                    answerCount) + '/?sort=SUBMIT_DATE&isAnswered=true'
                questionScore = getOneWeekQuestion(urlAnswered, ASIN, answerCount, questionDate)
                score += questionScore
        print(score)

        # 产品打包
        data = {
            'ASIN': ASIN,
            'title': title,
            'shop': shop,
            'Price': Price,
            'ListPrice': ListPrice,
            'reviews': reviews,
            'answered': answered,
            'seller': seller,
            'stars': stars,
            'ItemWeight': ItemWeight,
            'ShippingWeight': ShippingWeight,
            'bullets': bulletsList,
            'keywords': keywords,
            'Productdescription': Productdescription,
            'DateFirstAvailable': DateFirstAvailable,
            'Best0SellersRank': Best,
            'category': Best1,
            'score': score,
            'images': images,
            'url': fullUrl,
            'site': 8,
            'source_type': 1,
        }
        global count_
        count_ += 1
        # data = json.dumps(data)
        print(data)
        print('爬取：', count_, '条')
        POST_API(data)


def POST_API(data):
    data = dict(data)
    data_json = json.dumps(data)
    # data_json = parse.quote(data_json)
    json_text = requests.post(
        url='http://third.gets.com/api/index.php?act=insertAmazonProductCube&sec=20171212getscn&debug=1',
        data=data_json)
    print(json_text.text)

#
# if __name__ == "__main__":
#     while True:
#         # infoParse()
#         parse_()

if __name__ == '__main__':
    while True:
        for i in range(10):
            p = multiprocessing.Process(target=parse_)
            p.start()
        p.join()


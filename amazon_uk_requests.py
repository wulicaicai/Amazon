import random
import string

import requests, re, json, time, datetime, multiprocessing
from lxml import etree
from user_agent import generate_user_agent
from urllib import parse

count_ = 0


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
    month_re = re.compile(r'([a-zA-Z]+)')
    DateFirstAvailableMonth = month_re.findall(dateandtime)[0]
    month_num = month.index(DateFirstAvailableMonth)
    month_num += 1
    # 抓取日期 转为整数
    if signal == 1:
        date_re = re.compile(r'(\d+)[a-zA-Z].*?')
        date_num = int(date_re.findall(dateandtime)[0])
    elif signal == 2:
        date_re = re.compile(r'(\d+) [a-zA-Z].*?')
        date_num = int(date_re.findall(dateandtime)[0])
    # 抓取年份 转为整数
    year_re = re.compile(r'.*?(\d{4})')
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
        req = s.get(url, headers=headers, timeout=10)
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
            review_time = ret.replace('.', '').split(" ")
            review_time = review_time[2] + '-' + review_time[1] + '-' + (
                '0' + review_time[0] if int(review_time[0]) < 10 else review_time[0])
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
            question_time = question_time[2] + '-' + question_time[1] + '-' + question_time[0]
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
    print('GET_API')
    start_urls = []
    info = requests.get(
        url='http://testthird.gets.com:8383/api/index.php?act=getAmazonCubeinfo&sec=20171212getscn&site=3',
        timeout=30).text
    infoUrl = json.loads(info.encode('utf-8').decode('utf-8-sig')) #
    for i in infoUrl:  # 美国德国英国每个大类url 都有两页,所以我拼接一下,再去获取所有产品的url 交由parse方法完成
        start_urls.append(i + '/?pg=1')
        start_urls.append(i + '/?pg=2')
    return start_urls


def parse_():
    print('parse_')
    start_urls = GET_API()
    fullUrlList = []
    url = 'https://www.amazon.co.uk'
    for start_url in start_urls:
        # 因为识别机器人，所以用 getHtmlCallbak 去获取数据
        s = requests.session()
        info = getHtmlCallbak(start_url, s)
        sonXpath = etree.HTML(info)
        if sonXpath.xpath("//ol[@id='zg-ordered-list']/li"):
            sonUrl = sonXpath.xpath("//ol[@id='zg-ordered-list']/li/span/div/span/a/@href")
            for sonurl in sonUrl:
                fullUrl = url + sonurl
                fullUrlList.append(fullUrl)
    infoParse(fullUrlList)


def infoParse(fullUrlList):
# def infoParse():
    for fullUrl in fullUrlList:
    # for fullUrl in ['https://www.amazon.co.uk/AmazonBasics-Rooftop-Cargo-Carrier-litres/dp/B072ZHRDMZ/ref=zg_bsnr_automotive_6/258-2610431-8856422?_encoding=UTF8&psc=1&refRID=DXE3JJ7T7WBSR5GNE0AA']:
        print('infoParse', fullUrl)
        # 抓取 Asin 产品标志
        ASIN_re = re.compile(r'dp/(.*?)/ref')
        ASIN = ASIN_re.findall(fullUrl)[0]

        host = parse.urlparse(fullUrl).scheme + '://' + parse.urlparse(fullUrl).netloc + '/'
        if '&url=' in fullUrl:  # 判断是否有"&url=" 字符串
            proUrlStr = fullUrl.split('&url=')  # 根据url=进行切片
            fullUrl = parse.unquote(proUrlStr[1])  # 切片取值
        fullUrl = fullUrl.split('/ref')[0] + '?psc=1'
        fullUrl = '/dp' + fullUrl.split('/dp')[1]
        fullUrl = host + fullUrl
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
                '//div[@id="superLeafGameReviews_feature_div"]/div[@id="averageCustomerReviews"]/span[@class="a-declarative"]/span[@id="acrPopover"]/span[1]/a/i[1]/span/text()  | //div[@id="averageCustomerReviews_feature_div"]/div[@id="averageCustomerReviews"]/span[@class="a-declarative"]/span[@id="acrPopover"]/span[1]/a/i[1]/span/text()'):
            stars = infoXpath.xpath(
                '//div[@id="superLeafGameReviews_feature_div"]/div[@id="averageCustomerReviews"]/span[@class="a-declarative"]/span[@id="acrPopover"]/span[1]/a/i[1]/span/text()  | //div[@id="averageCustomerReviews_feature_div"]/div[@id="averageCustomerReviews"]/span[@class="a-declarative"]/span[@id="acrPopover"]/span[1]/a/i[1]/span/text()')[
                0]
            stars_re = re.compile(r'(.*?) out of.*?')
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
                '//div[@class="feature"]/div[@id="averageCustomerReviews"]/span[@class="a-declarative"]/a[@id="acrCustomerReviewLink"]/span[@id="acrCustomerReviewText"]/text()')
            reviews_re = re.compile('(\d+) .*?')
            reviews = reviews_re.findall(reviews[0])
            reviews = int(reviews[0])
        else:
            reviews = ' '
        # 回复数
        if infoXpath.xpath('//a[@id="askATFLink"]/span'):  # 获取回复数
            answered = infoXpath.xpath('//a[@id="askATFLink"]/span/text()')[0].strip()
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
        Price_re = re.compile(r'<span id="priceblock_ourprice" class="a-size-medium a-color-price priceBlockBuyingPriceString">(.*?)</span>')
        Price = Price_re.findall(info)
        if Price == []:
            Price =' '
        else:
            Price = Price[0]
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
        if infoXpath.xpath('//div[@class="a-section a-spacing-small"]/p/text()'):
            Productdescription = infoXpath.xpath('//div[@class="a-section a-spacing-small"]/p/text()')[0].replace('\n',
                                                                                                                  '').replace(
                '\t', '').strip()
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
            r'<span class="olp-padding-right"><a href=".*?">(.*?)&nbsp;new</a>&nbsp;from&nbsp;<span .*?</span></span>')
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
                if Name == 'ItemWeight':  # 产品重量
                    ItemWeight = tr.xpath('./td[3]/text()')
                    if ItemWeight != []:
                        ItemWeight[0].replace('\n', '').replace(' ', '').replace('(', '')
                    else:
                        ItemWeight = ' '
                if Name == 'ShippingWeight':  # 包装重量
                    ShippingWeight = tr.xpath('./td[2]/text()')[0].replace('\n', '').replace(' ', '').replace('(', '')
                if Name == 'DateFirstAvailable' or Name == 'DatefirstlistedonAmazon':  # 上架时间
                    # 抓取上架时间
                    DateFirstAvailable = tr.xpath('./td[2]/text()')
                    DateFirstAvailable = DateFirstAvailable[0].replace('\n', '').replace(' ', '')
                    times, DateFirstAvailable = dateTime(DateFirstAvailable, 1)

        # 无表抓取
        if infoXpath.xpath(
                '//ul[@class="a-unordered-list a-nostyle a-vertical a-spacing-none"] | //td[@class="bucket"]/div[@class="content"]/ul'):  # 验证一下table 里有ul 这个class吗
            # 上架时间
            DateFirstAvailable_re = re.compile(r"Date first available at Amazon\.co\.uk:</b> (.*?)</li>")
            DateFirstAvailable = DateFirstAvailable_re.findall(info)
            if DateFirstAvailable:
                times, DateFirstAvailable = dateTime(DateFirstAvailable[0], 2)
            else:
                times = None
            # 包装重量
            ShippingWeight_re = re.compile(r'<li><b>Boxed-product Weight:</b> (.*?)</li>')
            ShippingWeight = ShippingWeight_re.findall(info)
            if ShippingWeight:
                ShippingWeight = ShippingWeight[0]

            # 产品重量
            ItemWeight_re = re.compile(r"Item Weight:\s+</span>\s+<span>(.*?)\(<a.*?</span>")
            ItemWeight = ItemWeight_re.findall(info)
            if ItemWeight:
                ItemWeight = ItemWeight[0]

        span_two = re.compile(
            r'<span class="zg_hrsr_rank">(.*?)</span>\s+<span class="zg_hrsr_ladder">(.*?)&nbsp;<a href="(.*?)">(.*?)</a></span>')
        span_2 = span_two.findall(info)
        if span_2 == []:
            print('排名等处理空数据')
        else:
            for sp in span_2:
                BestL = []
                Best.append(list(sp))
                BestL.append(list(sp)[2])
                Best1.append(BestL)
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
            'bullets': bullet,
            'keywords': keywords,
            'Productdescription': Productdescription,
            'DateFirstAvailable': DateFirstAvailable,
            'Best0SellersRank': Best,
            'category': Best1,
            'score': score,
            'images': images,
            'url': fullUrl,
            'site': 3,
            'source_type': 1,
        }

        global count_
        count_ += 1
        print(fullUrl)
        print('爬取：', count_, '条')
        print(data)
        POST_API(data)


def POST_API(data):
    data = dict(data)
    data_json = json.dumps(data)
    # data_json = parse.quote(data_json)
    s = requests.session()
    s.keep_alive = False
    json_text = s.post(
        url='http://third.gets.com/api/index.php?act=insertAmazonProductCube&sec=20171212getscn&debug=1',
        data=data_json)
    print(json_text.text)


# if __name__ == "__main__":
#
#     # infoParse()
#     parse_()

if __name__ == '__main__':
    while True:
        for i in range(10):
            p = multiprocessing.Process(target=parse_)
            p.start()
        p.join()

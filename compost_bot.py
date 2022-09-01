import json
import urllib.request
import logging
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

channel_token = '<token>'
DB_HOST = '<db.host>'
DB_USER = '<db.user>'
DB_PASSWORD = '<db.passwd>'
DB_NAME = '<db.name>'
conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME)

def lambda_handler(event, context):
	logger.info(event)
	for message_event in json.loads(event['body'])['events']:
		logger.info(json.dumps(message_event))
		
		url = 'https://api.line.me/v2/bot/message/reply'
		headers = {
			'Content-Type': 'application/json',
			'Authorization': 'Bearer ' + channel_token
		}
		rc_text = message_event['message']['text']
		user_id = message_event['source']['userId']
		
		with conn.cursor() as cur:
			sql = f"SELECT count(id) FROM users WHERE client_id='{user_id}' and is_activated=1;"
			cur.execute(sql)
			for count in cur:
				ac_check = count[0]
		
		auth = ['許諾', '承認', '承諾', '承知', '了承', '了解', '許可', '容認', '認可', '受諾', '是認', '認証']
		if rc_text in auth and ac_check == 0:
			with conn.cursor() as cur:
				sql = f"INSERT INTO users (client_id, is_activated) values ('{user_id}', 1)"
				cur.execute(sql)
				conn.commit()
				rp_text = '許諾ありがとうございます。\n続けて利用方法をご案内いたします。\n\n★利用開始方法★\n「登録」の２文字を入力した後、\nスペースを空けてから好きなニックネームを送信して、\nご利用を開始してください（任意）。\n（例）登録　集計くん\n\n★投入量登録★\n生ごみの投入量をg(グラム)単位で入力し送信してください。\n例えば、投入量が100gの場合、「100」と入力します。\n送信していただくとコンポストに投入した量を登録できます。\n登録が完了すると返信が届きます。\n注：一日に登録できるのは5回までとなります。\n\n★投入量確認★\n「今月」\nを送信していただくと今月の投入量を確認できます。\n※毎月1日から月末までの合計になります\n\n「合計」\nを送信していただくとご利用開始から今日までの投入量の合計を確認できます。\n\n★お困りの際は★\nご利用しているお客様が多い時間帯は返信が遅れることがあります。返答がないときは時間を開けて再度送信をお願い致します。'
		elif ac_check == 1:
			if rc_text in auth:
				rp_text = 'お客様はすでに弊社の個人情報保護方針に許諾しております。\n引き続きご利用お願いいたします。'
			elif '登録' in rc_text and len(rc_text)>3 and ac_check > 0:
				sql = f"SELECT count(user_name) FROM line_bot.users WHERE client_id='{user_id}';"
				with conn.cursor() as cur:
					cur.execute(sql)
					for count in cur:
						cnt_name = count[0]
						break
		
				if cnt_name == 0:
					if rc_text[2] == ' ' or rc_text[2] == '　':
						user_name = rc_text[3:]
						with conn.cursor() as cur:
							sql = f"UPDATE users set user_name='{user_name}' where client_id='{user_id}'"
							cur.execute(sql)
							conn.commit()
							rp_text = f"{user_name}様、\nニックネーム登録確認できました、\n引き続きご利用よろしくお願いいたします。"
					else:
						rp_text = '申し訳ございません、お客様の入力情報を登録できませんでした。\n「登録」の２文字を入力した後、スペースを空けてからニックネームを入れて送信すると利用開始できます。\n（例）登録　集計くん\n※お客様の本名は入力しないでください。'
				else:
					rp_text = 'お客様の情報はすでに登録完了しております、引き続きご利用ください。'
			
			else:
				if rc_text.isdigit():
					with conn.cursor() as cur:
						sql = f"SELECT count(id) FROM daily_waste WHERE client_id='{user_id}' and curdate()=date(created_at);"
						cur.execute(sql)
						for count in cur:
							cnt_waste = count[0]
							break
						# rp_text = cnt_waste

					if cnt_waste < 5:
						value = int(rc_text)
						sql = f"INSERT INTO daily_waste (client_id, waste_value) values ('{user_id}', '{value}');"
						# data =[(user_id, value)]
						with conn.cursor() as cur:
							cur.execute(sql)
							conn.commit()
							rp_text = '本日の排出量登録ありがとうございます！\n引き続きご利用よろしくお願いいたします。'
					else:
						rp_text = 'お客様は本日すでに5回以上投入量を入力しております。また明日引き続きご利用お願いいたします。'
						
				elif rc_text[0].isdigit() and len(rc_text)>2:
					try:
						rc_text = float(rc_text)
						value = round(rc_text)
						with conn.cursor() as cur:
							sql = f"SELECT count(id) FROM daily_waste WHERE client_id='{user_id}' and curdate()=date(created_at);"
							cur.execute(sql)
							for count in cur:
								cnt_waste = count[0]
								break
						if cnt_waste < 5:
							value = int(rc_text)
							sql = f"INSERT INTO daily_waste (client_id, waste_value) values ('{user_id}', '{value}');"
							# data =[(user_id, value)]
							with conn.cursor() as cur:
								cur.execute(sql)
								conn.commit()
								rp_text = '本日の排出量登録ありがとうございます！\n引き続きご利用よろしくお願いいたします。'
						else:
							rp_text = 'お客様は本日すでに5回以上投入量を入力しております。また明日引き続きご利用お願いいたします。'
							
					except:
						rp_text = '申し訳ございません、お客様の入力情報を登録できませんでした。\n「300（例）」\nを送信していただくと排出量を登録できます。'
				elif rc_text[0].isdigit():
					rp_text = '申し訳ございません、お客様の入力情報を登録できませんでした。\n「300（例）」\nを送信していただくと排出量を登録できます。'
				
				elif rc_text == '今月':
					sql = f"SELECT waste_value FROM daily_waste WHERE DATE_FORMAT(created_at, '%Y%m') = DATE_FORMAT(NOW(), '%Y%m') AND client_id='{user_id}';"
					with conn.cursor() as cur:
						cur.execute(sql)
						vol_sum = 0
						for vol in cur:
							vol_sum += int(vol[0])
					rp_text = f'お客様の今月の排出量：{vol_sum}g'
				elif rc_text=='合計':
					sql = f"SELECT waste_value FROM daily_waste WHERE client_id='{user_id}';"
					with conn.cursor() as cur:
						cur.execute(sql)
						vol_sum = 0
						for vol in cur:
							vol_sum += int(vol[0])
					rp_text = f'お客様の今日までの排出量：{vol_sum}g'
				elif rc_text != '利用方法':
					rp_text = '申し訳ございません。お客様の入力情報が認識できませんでした。\nメニューの「利用方法」をご確認ください。'
		
		elif rc_text != '利用方法':
			rp_text = '弊社の個人情報保護方針をご確認の上、本チャットルームにて「許諾」を送信してご利用を開始してください。'
				
		data = {
			'replyToken': message_event['replyToken'],
			'messages': [
				{
					"type": "text",
					"text": rp_text
				}
			]
		}
		
		req = urllib.request.Request(url=url, data=json.dumps(data).encode('utf-8'), method='POST', headers=headers)
		
		with urllib.request.urlopen(req) as res:
			logger.info(res.read().decode("utf-8"))
		
	# TODO implement
	return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda!')
	}
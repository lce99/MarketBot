"""텔레그램 봇 - 명령어 핸들러 + 자동 리포트 전송"""

import logging

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

from src.config import COUNTRIES, SECTORS, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from src.reporter import (
    format_country_detail,
    format_daily_report,
    format_sector_detail,
)

logger = logging.getLogger(__name__)

# 국가 이름 → 코드 매핑 (한글 입력 지원)
COUNTRY_NAME_MAP = {}
for code, info in COUNTRIES.items():
    COUNTRY_NAME_MAP[code] = code
    COUNTRY_NAME_MAP[code.lower()] = code
    COUNTRY_NAME_MAP[info["name_kr"]] = code

# 섹터 이름 매핑 (한글/영문 지원)
SECTOR_NAME_MAP = {}
for kr, en in SECTORS.items():
    SECTOR_NAME_MAP[kr] = kr
    SECTOR_NAME_MAP[en.lower()] = kr


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """봇 시작. 채팅 ID 알려줌."""
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        f"\U0001f4ca MarketBot 시작!\n\n"
        f"채팅 ID: {chat_id}\n"
        f"이 ID를 GitHub Secrets의 TELEGRAM_CHAT_ID에 설정하세요.\n\n"
        f"사용 가능한 명령어:\n"
        f"/report - 최신 종합 리포트\n"
        f"/sector 정보기술 - 특정 섹터 상세\n"
        f"/country 한국 - 특정 국가 상세\n"
        f"/trending - 글로벌 트렌딩 TOP 5\n"
        f"/abnormal - 비정상 급등/급락\n"
        f"/help - 도움말"
    )


async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """종합 리포트 전송."""
    messages = format_daily_report()
    for msg in messages:
        if msg.strip():
            await update.message.reply_text(msg)


async def cmd_sector(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """특정 섹터 상세."""
    if not context.args:
        sectors_list = "\n".join(f"  - {s}" for s in SECTORS.keys())
        await update.message.reply_text(
            f"사용법: /sector 섹터명\n\n사용 가능한 섹터:\n{sectors_list}"
        )
        return

    sector_input = " ".join(context.args)
    sector_name = SECTOR_NAME_MAP.get(sector_input, sector_input)

    msg = format_sector_detail(sector_name)
    await update.message.reply_text(msg)


async def cmd_country(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """특정 국가 상세."""
    if not context.args:
        countries_list = "\n".join(
            f"  - {info['flag']} {info['name_kr']} ({code})"
            for code, info in COUNTRIES.items()
        )
        await update.message.reply_text(
            f"사용법: /country 국가명\n\n사용 가능한 국가:\n{countries_list}"
        )
        return

    country_input = " ".join(context.args)
    country_code = COUNTRY_NAME_MAP.get(country_input, country_input.upper())

    msg = format_country_detail(country_code)
    await update.message.reply_text(msg)


async def cmd_trending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """글로벌 트렌딩 섹터 TOP 5."""
    messages = format_daily_report()
    if messages:
        # 첫 번째 메시지에 트렌딩 정보 포함
        await update.message.reply_text(messages[0])
    else:
        await update.message.reply_text("\u274c 데이터가 없습니다.")


async def cmd_abnormal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """비정상 급등/급락 종목."""
    messages = format_daily_report()
    # 마지막 메시지가 비정상 종목
    for msg in messages:
        if "\u26a0\ufe0f" in msg:
            await update.message.reply_text(msg)
            return
    await update.message.reply_text("\u2705 오늘 비정상 급등/급락 종목이 없습니다.")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """도움말."""
    await update.message.reply_text(
        "\U0001f4ca MarketBot - 글로벌 섹터 트렌드 분석\n\n"
        "명령어:\n"
        "/report - 최신 종합 리포트\n"
        "/sector 정보기술 - 특정 섹터의 국가별 상세\n"
        "/country 한국 - 특정 국가의 섹터별 상세\n"
        "/trending - 글로벌 트렌딩 섹터 TOP 5\n"
        "/abnormal - 비정상 급등/급락 종목\n\n"
        "자동 리포트: 매일 미국 장 마감 후 발송\n\n"
        "지원 국가: 미국, 한국, 중국, 일본, 베트남, 인도, 독일\n"
        "분석 섹터: GICS 11개 섹터"
    )


async def send_auto_report(token: str, chat_id: str):
    """GitHub Actions에서 호출하여 자동 리포트 전송."""
    from telegram import Bot
    bot = Bot(token=token)
    messages = format_daily_report()
    for msg in messages:
        if msg.strip():
            await bot.send_message(chat_id=chat_id, text=msg)
    logger.info("자동 리포트 전송 완료")


def run_bot():
    """봇을 polling 모드로 실행 (로컬 테스트용)."""
    if not TELEGRAM_BOT_TOKEN:
        print("TELEGRAM_BOT_TOKEN 환경변수를 설정하세요.")
        return

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("report", cmd_report))
    app.add_handler(CommandHandler("sector", cmd_sector))
    app.add_handler(CommandHandler("country", cmd_country))
    app.add_handler(CommandHandler("trending", cmd_trending))
    app.add_handler(CommandHandler("abnormal", cmd_abnormal))
    app.add_handler(CommandHandler("help", cmd_help))

    print("\U0001f916 MarketBot 실행 중... (Ctrl+C로 종료)")
    app.run_polling()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_bot()

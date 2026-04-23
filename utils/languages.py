# ─── Language strings ─────────────────────────────────────────
# Add new languages by adding a new key block below

STRINGS = {
    "en": {
        "choose_language": "🌐 Please choose your language:",
        "language_set": "✅ Language set to English!",
        "welcome": (
            "👋 <b>Welcome!</b>\n\n"
            "Use /refer to get your personal referral link.\n"
            "📊 Use /leaderboard to see top referrers.\n"
            "📈 Use /mystats to see your stats."
        ),
        "not_joined": "❌ You must join the channel before using this bot!",
        "join_btn": "📢 Join Channel",
        "verify_btn": "✅ I've Joined — Verify",
        "verified": "✅ Verified! You can now use the bot.",
        "not_verified": "❌ You haven't joined yet. Please join and try again.",
        "refer_msg": "🔗 <b>Your Referral Link:</b>\n<code>{link}</code>\n\n{caption}\n\n👥 You've referred <b>{count}</b> user(s).",
        "mystats": "📊 <b>Your Stats</b>\n\n👤 Name: {name}\n🔗 Referrals: <b>{count}</b>\n🏆 Top referrer has: <b>{top}</b>",
        "no_referrals": "You haven't referred anyone yet.",
        "myreferrals_header": "👥 <b>People you referred ({count}):</b>\n",
        "powered_by": "\n\n<i>Made via {main_bot}\nPowered by @RoyalityBots</i>",
        "not_started": "❌ You haven't started the bot yet. Use /start first.",
        "cmd_disabled": "❌ This command is disabled by the bot owner.",
    },
    "hi": {
        "choose_language": "🌐 कृपया अपनी भाषा चुनें:",
        "language_set": "✅ भाषा हिंदी में सेट हो गई!",
        "welcome": (
            "👋 <b>स्वागत है!</b>\n\n"
            "अपना रेफरल लिंक पाने के लिए /refer करें।\n"
            "📊 टॉप रेफरर देखने के लिए /leaderboard करें।\n"
            "📈 अपने stats देखने के लिए /mystats करें।"
        ),
        "not_joined": "❌ बॉट use करने से पहले चैनल join करें!",
        "join_btn": "📢 चैनल Join करें",
        "verify_btn": "✅ Join हो गया — Verify करें",
        "verified": "✅ Verified! अब आप बॉट use कर सकते हैं।",
        "not_verified": "❌ आपने अभी join नहीं किया। Join करके दोबारा try करें।",
        "refer_msg": "🔗 <b>आपका Referral Link:</b>\n<code>{link}</code>\n\n{caption}\n\n👥 आपने <b>{count}</b> यूज़र refer किए हैं।",
        "mystats": "📊 <b>आपके Stats</b>\n\n👤 नाम: {name}\n🔗 Referrals: <b>{count}</b>\n🏆 Top referrer के पास: <b>{top}</b>",
        "no_referrals": "आपने अभी तक किसी को refer नहीं किया।",
        "myreferrals_header": "👥 <b>आपके द्वारा refer किए गए लोग ({count}):</b>\n",
        "powered_by": "\n\n<i>{main_bot} के ज़रिए बनाया गया\nPowered by @RoyalityBots</i>",
        "not_started": "❌ आपने बॉट start नहीं किया। पहले /start करें।",
        "cmd_disabled": "❌ यह command बॉट owner ने बंद कर दी है।",
    }
}


def t(lang: str, key: str, **kwargs) -> str:
    """Get a translated string, fallback to English."""
    text = STRINGS.get(lang, STRINGS["en"]).get(key, STRINGS["en"].get(key, key))
    if kwargs:
        text = text.format(**kwargs)
    return text

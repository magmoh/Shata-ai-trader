# SHATA AI TRADER — سجل الاكتشافات

سطر واحد لكل اكتشاف. يُملأ عند كل دورة مراجعة. بعد ثلاث دورات تعرف بالأرقام أي مراجع يستحق مقعده.

**الأعمدة:**
- `Evidence`: `executable` (اختبار يفشل مرفق) · `design` (مراجعة تصميم بلا تنفيذ) · `self-attack` (كتبه بانِي الكود — وزن أدنى)
- `Blind`: هل كان المراجع أعمى عن التقارير السابقة
- `Status`: `open` · `patched` · `verified` (اختبار انحدار كتبه طرف غير البانِي ويمر) · `rejected` (غير قابل لإعادة الإنتاج)

---

## v0.4 → v0.7 (تاريخي — مراجع واحد)

| ID | الإصدار | المراجع | Blind | الخطورة | العنوان | Evidence | Blocker | Status |
|---|---|---|---|---|---|---|---|---|
| C-1 | v0.4 | Claude | لا | CRITICAL | `recover_intent` ينهار على 11/16 حالة دائمة؛ لا إقلاع ثانٍ مع مركز `PROTECTED` | executable | YES | verified |
| C-2 | v0.4 | Claude | لا | CRITICAL | «الأمر غير موجود» تُكتب `CANCELED` نهائية | executable | YES | verified |
| C-3 | v0.4 | Claude | لا | CRITICAL | بوابة Cold Boot قابلة للتجاوز بالكامل | executable | YES | verified |
| H-1..H-10 | v0.4 | Claude | لا | HIGH | سباق redrive · تزامن العقد · `StaleEpoch` هارب · انقطاع المرساة · قفل عبر I/O · دفتر غير مُسيَّج · صف تالف · واقعية المحاكي · قفل الدفتر · تنفيذ جزئي يوقف النظام | executable | — | verified |
| A5 | v0.5 | Claude | لا | CRITICAL | `PROTECTED` كاذبة: نقص كمية · تداخل خروج اضطراري · إلغاء خارجي · لا مُصالح داخل الجلسة | executable | YES | verified |
| A1-A7 | v0.5 | Claude | لا | HIGH | المرساة تكتب فوق الشاهد · `stop/start` يقتل المجدِّد · سلطة الدفتر مربوطة بالكائن · `LeaseUnavailable` يمنع إعادة التشغيل · حدث يتجاوز البوابة | executable | NO | verified |
| H-1 | v0.6 | Claude | لا | HIGH | الشاهد يُعاد ختمه بأي إلحاق لاحق | executable | NO | verified |
| H-2 | v0.6 | Claude | لا | HIGH | `max_verification_age_seconds` غير مفروض؛ تقدّم المشرف بلا مراقبة | executable | NO | verified |
| M-1 | v0.6 | Claude | لا | MEDIUM | تذكرة مهجورة في منظّم المعدل تُجمّد كل نداءات المنصة | executable | NO | verified |
| M-2 | v0.6 | Claude | لا | MEDIUM | حدث منصة مشوَّه يخرج باستثناء | executable | NO | verified |
| N1 | v0.7 | Claude | لا | HIGH | إعادة ربط قدرة الـRuntime تفتح البوابة بلا إقلاع بارد | executable | **YES** | patched (v0.8) |
| N3 | v0.7 | Claude | لا | HIGH | لا أحد يراقب المراقب في المسار الخامل | executable | NO | patched (v0.8) |
| N2 | v0.7 | Claude | لا | MEDIUM | لا ارتفاع في الشاهد: اقتطاع التاريخ مقبول | executable | NO | patched (v0.8) |

---

## v0.8 (جارٍ)

**النطاق المقترح:** N1 · N3 · N2 · `supervisor_kill_chaos_1000.py`

**الحالة:** حزمة مرشحة جاهزة — `SHATA_AI_TRADER_PHASE0_v0_8_BLIND_REVIEW.zip`

| ID | المراجع | Blind | الخطورة | العنوان | Evidence | Blocker | Status |
|---|---|---|---|---|---|---|---|
| B-1 | Claude | لا | HIGH | البوابة تبقى مفتوحة حين تموت **كل** خيوط الإشراف: `ready` يقرأ False لكن لا أحد يبقى ليستدعي `revoke_boot_authority` | self-attack (كشفه مُسخِّر البند الرابع أثناء البناء) | — | patched (`engine.gate_open` + مِجَس صحة حي) |
| B-2 | Claude | لا | MEDIUM | شاهد بلا `height` يمرّ كسجل قديم = هجوم تخفيض | self-attack | — | patched |
| B-3 | Claude | لا | MEDIUM | فحص صحة مشرف العقد بالتقدّم وحده يُطلق إنذارًا كاذبًا تحت TTL قصير (`chaos_1000` 1/1000) | self-attack | — | patched (يفحص الثابت الحقيقي: هل السلطة ما زالت صالحة) |
| | Gemini | نعم | | *بانتظار التقرير* | | | NOT REVIEWED |
| | ChatGPT | لا | | *بانتظار التقرير* | | | NOT REVIEWED |

---

## لوحة المساهمة

| المراجع | اكتشافات Critical/High | منها `executable` | منها `rejected` | إصدارات شارك فيها |
|---|---|---|---|---|
| Claude | 16 | 16 | 0 | v0.4–v0.7 (مهاجم) · v0.8 (بانٍ — 3 اكتشافات `self-attack`) |
| Gemini | — | — | — | — |
| ChatGPT | — | — | — | — |

## أسطح لم تُهاجَم بعد

يُحدَّث كل دورة. هذه قائمة الديون لا قائمة الإنجازات.

- ~~قتل الخيوط الإشرافية أثناء الفوضى~~ ✅ `supervisor_kill_chaos_1000.py` في v0.8
- `fail_emergency_exit` — حقنة عطل مكتوبة وغير مستخدمة في أي سكربت فوضى (ما زالت دَينًا)
- `RateGovernorTimeout` تُرفع ولا تُلتقط باسمها: مهلة على نداء حماية تبدو كفشل حماية عادي
- `cancel_protection_by_client_id` بلا مستدعٍ في المحرك
- فشل الخروج الاضطراري تحت مراكز متعددة
- ساعة النظام: القفز للخلف/الأمام أثناء التحقق من الحماية والعقد
- امتلاء القرص / فشل `fsync` أثناء كتابة WAL أو التدقيق
- بوابة متجه الميزات (Phase 1) — لم تُكتب بعد

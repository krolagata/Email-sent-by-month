SELECT
  sent_month,
  id_account,
  ROUND(
    CAST(sent_msg_count_for_account AS FLOAT64) / sent_msg_count_from_this_month * 100,
    4
  ) AS sent_msg_percent_from_this_month,
  first_sent_date,
  last_sent_date
FROM (
  SELECT
    DATE_TRUNC(DATE_ADD(ss.date, INTERVAL es.sent_date DAY), MONTH) AS sent_month,
    es.id_account,
    es.id_message,
    DATE_ADD(ss.date, INTERVAL es.sent_date DAY) AS full_sent_date,
    COUNT(DISTINCT es.id_message) OVER (PARTITION BY DATE_TRUNC(DATE_ADD(ss.date, INTERVAL es.sent_date DAY), MONTH)) AS sent_msg_count_from_this_month,
    COUNT(DISTINCT es.id_message) OVER (PARTITION BY DATE_TRUNC(DATE_ADD(ss.date, INTERVAL es.sent_date DAY), MONTH), es.id_account) AS sent_msg_count_for_account,
    MIN(DATE_ADD(ss.date, INTERVAL es.sent_date DAY)) OVER (PARTITION BY DATE_TRUNC(DATE_ADD(ss.date, INTERVAL es.sent_date DAY), MONTH), es.id_account) AS first_sent_date,
    MAX(DATE_ADD(ss.date, INTERVAL es.sent_date DAY)) OVER (PARTITION BY DATE_TRUNC(DATE_ADD(ss.date, INTERVAL es.sent_date DAY), MONTH), es.id_account) AS last_sent_date
  FROM `data-analytics-mate.DA.email_sent` AS es
  JOIN `data-analytics-mate.DA.account_session` AS ac ON es.id_account = ac.account_id
  JOIN `data-analytics-mate.DA.session` AS ss ON ac.ga_session_id = ss.ga_session_id
  WHERE es.sent_date IS NOT NULL AND es.sent_date >= 0
)
GROUP BY sent_month, id_account, sent_msg_count_for_account, sent_msg_count_from_this_month, first_sent_date, last_sent_date
ORDER BY sent_month, id_account

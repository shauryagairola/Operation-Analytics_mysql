SELECT * FROM operation_analytics.job_data;
use  operation_analytics;

#Case-1
#TASK 1-no.of jobs reviewed per hour per day
SELECT ds AS day,
Count(job_id) / (Sum(time_spent) / 3600) AS jobs_reviewed_per_hour
FROM job_data
GROUP BY day ORDER BY day;

#TASK 2- 7 day rolling avg of throughput
SELECT ds AS day,new.throughput,
avg(new.throughput) OVER ( ORDER BY ds rows BETWEEN 6 PRECEDING AND CURRENT row ) AS
7_day_avg_of_throughput
FROM
( SELECT ds, count(job_id) / sum(time_spent) AS throughput FROM job_data GROUP BY ds ) AS new
GROUP BY ds;

#TASK 3- % share of each language
SELECT language,
count(job_id) as no_of_jobs,
count(job_id)*100 / sum(count(job_id)) OVER() as percentage_share
FROM job_data
GROUP by language;

#TASK 4- Identify duplicate rows
SELECT a.ds,
a.job_id,
a.actor_id,
a.event,
a.language,
a.time_spent,
a.org,
CASE when a.duplicates = 1 then "No Duplicate" else "Duplicate" end as Duplicate
FROM
( SELECT *, row_number() OVER (partition by ds, job_id, actor_id, event, language, time_spent, org)
as duplicates FROM job_data ) as a ;

#Case-2
#task 5- weekly user engagement
SELECT WEEK(STR_TO_DATE(occurred_at, '%Y-%m-%d')) AS week,
COUNT(DISTINCT user_id) AS weekly_engaged_users
FROM events
GROUP BY week
ORDER BY week;

#TASK 6- user growth
SELECT WEEK(STR_TO_DATE(created_at,'%Y-%m-%d')) AS week_num, COUNT(user_id) as NoOfUsers,
COUNT(USER_ID) - LAG(COUNT(user_id),1) OVER(ORDER BY WEEK(STR_TO_DATE(created_at,'%Y-%m-%d')))
AS user_growth FROM users
GROUP BY week_num order by week_num;

#TASK 7- weekly user retention
SELECT
WEEK(STR_TO_DATE(u.activated_at, '%Y-%m-%d')) AS
signup_week,
WEEK(STR_TO_DATE(e.occurred_at, '%Y-%m-%d')) AS event_week,
COUNT(DISTINCT e.user_id) AS retained_users FROM users u
JOIN events e ON u.user_id = e.user_id
WHERE WEEK(STR_TO_DATE(e.occurred_at, '%Y-%m-%d'))- WEEK(STR_TO_DATE(u.activated_at, '%Y-%m-%d')) = 0 
GROUP BY signup_week, event_week
ORDER BY signup_week, event_week;

#TASK 8-weekly engagement per device
SELECT WEEK(STR_TO_DATE(occurred_at, '%Y-%m-%d')) AS week,device,
COUNT(DISTINCT user_id) AS engaged_users FROM events
GROUP BY week, device
ORDER BY week, device;

#TASK 9- email engagement
SELECT
action,
COUNT(DISTINCT user_id) AS engaged_users
FROM email_events GROUP BY action;



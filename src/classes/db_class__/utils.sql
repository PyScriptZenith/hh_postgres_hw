"""SQL код для написания функций"""

1 -- get_companies_and_vacancies_count()
-- получает список всех компаний и количество вакансий у каждой компании.

SELECT company_name, COUNT (*) FROM vacancies
GROUP BY company_name
ORDER BY COUNT (*) DESC

2 --get_all_vacancies()

--получает список всех вакансий с указанием названия компании,
--названия вакансии и зарплаты и ссылки на вакансию.

SELECT * FROM vacancies

3 -- get_avg_salary()
-- получает среднюю зарплату по вакансиям.

SELECT AVG(pay) as avg_pay FROM vacancies

4.--get_vacancies_with_higher_salary()
--получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.

SELECT vacancy_name, pay FROM vacancies
WHERE pay > (SELECT AVG(pay) as avg_pay FROM vacancies)
ORDER BY pay

5.--get_vacancies_with_keyword()
-- получает список всех вакансий,
-- в названии которых содержатся переданные в метод слова, например python.

SELECT * FROM vacancies
WHERE vacancy_name LIKE '%верстал%'



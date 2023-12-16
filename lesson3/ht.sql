-- Завдання на SQL до лекції 03.

-- 1. Вивести кількість фільмів в кожній категорії. Результат відсортувати за спаданням.
SELECT COUNT(f.film_id) AS film_count, c."name" 
FROM film f
JOIN film_category fc ON fc.film_id = f.film_id
JOIN category c ON c.category_id = fc.category_id
GROUP BY c.category_id
ORDER BY COUNT(f.film_id) DESC;

-- 2. Вивести 10 акторів, чиї фільми брали на прокат найбільше. Результат відсортувати за спаданням.
SELECT COUNT(r.rental_id)  AS rental_count, a.first_name, a.last_name
FROM rental r
JOIN inventory i ON i.inventory_id = r.inventory_id
JOIN film f ON i.film_id = f.film_id
JOIN film_actor fa ON fa.film_id = f.film_id
JOIN actor a ON a.actor_id = fa.actor_id
GROUP BY a.actor_id
ORDER BY COUNT(r.rental_id) DESC
LIMIT 10;

-- 3. Вивести категорія фільмів, на яку було витрачено найбільше грошей в прокаті.
SELECT SUM(amount) AS sum_amount, c."name" AS category_name
FROM payment p
JOIN rental r ON r.rental_id = p.rental_id
JOIN inventory i ON i.inventory_id = r.inventory_id
JOIN film f ON i.film_id = f.film_id
JOIN film_category fc ON fc.film_id = f.film_id
JOIN category c ON c.category_id = fc.category_id
GROUP BY c.category_id
ORDER BY SUM(amount) DESC
LIMIT 1;

-- 4. Вивести назви фільмів, яких не має в inventory. Запит має бути без оператора IN.
SELECT film.title
FROM film
WHERE NOT EXISTS (
    SELECT 1
    FROM inventory
    WHERE inventory.film_id = film.film_id
);

-- 5. Вивести топ 3 актори, які найбільше з'являлись в категорії фільмів “Children”.
SELECT a.first_name, a.last_name, COUNT(f.film_id) AS film_count
FROM category c
JOIN film_category fc ON c.category_id = fc.category_id
JOIN film f ON f.film_id = fc.film_id
JOIN film_actor fa ON fa.film_id = f.film_id
JOIN actor a ON a.actor_id = fa.actor_id
WHERE c."name" = 'Children'
GROUP BY a.actor_id, a.first_name, a.last_name
ORDER BY film_count DESC
LIMIT 3;

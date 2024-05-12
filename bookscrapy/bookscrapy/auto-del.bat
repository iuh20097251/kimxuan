docker image rm -f bookhomework
docker system prune
docker volume rm -f book_bookpy
docker build -t bookhomework .

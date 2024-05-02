sudo ./server $1 0 &
sleep 5
sudo ./server $1 1 &
sleep 5
sudo ./client $1 2 0
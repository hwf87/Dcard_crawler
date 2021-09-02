echo 'start dcard daily etl job'
source /Users/jackyfu/miniforge3/bin/activate
cd /Users/jackyfu/Desktop/hwf87_git/Dcard_crawler/
python3 utils.py
#END=$(date + "%Y%m%d, %H:%M")
echo 'end dcard daily etl job'


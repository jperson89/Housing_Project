import pyautogui

password = pyautogui.password(text='Please enter your MySQL password or type q to quit. Invalid entries will exit.',
                                  title='MySQL Password',
                                  default='',
                                  mask='*')

host = "localhost"
user = "root"
password = password

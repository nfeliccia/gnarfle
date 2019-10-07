from gnarfle_bst import cls
from gnarfle_bst import show_off
from gnarfle_indeed import gnarfle_indeed
from gnarfle_indeed_scramble_searcher import gnarfle_indeed_scramble_searcher

intro_text_file_name = r'.\input\intro.txt'
help_menu_file_name = r'.\input\help_menu.txt'
action_dict = {'qq': 0, 'quit': 0, 'ss': 1, 'scramble_searcher': 1, 'kk': 2, 'keywords': 2}
cls()
show_off(intro_text_file_name)
show_off(help_menu_file_name)

ui_continue = True
while ui_continue:
    ui = input("Main Menu - Enter Command:\t")
    take_action = action_dict.get(ui, 'help')
    if take_action == 'help':
        show_off(help_menu_file_name)
    elif not take_action:
        print("Thank you for Gnarfling... Come Back soon")
        ui_continue = False
    if take_action == 1:
        show_off(r'.\input\giss_intro.txt')
        gnarfle_indeed_scramble_searcher()
    if take_action == 2:
        gnarfle_indeed()

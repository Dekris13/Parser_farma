import os
import random




# Выбор рандомного User_agent для подключения
def User_agent_selection():

    #Читаем файл со списком доступных User Agents
    project_root_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(project_root_dir,  'user_agents/desktop_user_agents.txt'), 'r') as file:

        # Выбитаем случайный User Agent
        list_user_agents =  file.readlines()
        user_agent = random.choice(list_user_agents)

        # Указываем user agent в заголовках запроса перед выполнением запроса
        headers = {"User-Agent": user_agent}

        return headers

        




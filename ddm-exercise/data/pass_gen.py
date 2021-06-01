import random
import hashlib

NAMES = ['Sophia', 'Jackson', 'Olivia', 'Liam', 'Emma', 'Noah', 'Ava', 'Aiden', 'Isabella', 'Lucas', 'Mia', 'Caden', 'Aria', 'Grayson', 'Riley', 'Mason', 'Zoe', 'Elijah', 'Amelia', 'Logan', 'Layla', 'Oliver', 'Charlotte', 'Ethan', 'Aubrey', 'Jayden', 'Lily', 'Muhammad', 'Chloe', 'Carter', 'Harper', 'Michael', 'Evelyn', 'Sebastian', 'Adalyn', 'Alexander', 'Emily', 'Jacob', 'Abigail', 'Benjamin', 'Madison', 'James', 'Aaliyah', 'Ryan', 'Avery', 'Matthew', 'Ella', 'Daniel', 'Scarlett', 'Jayce', 'Maya', 'Mateo', 'Mila', 'Caleb', 'Nora', 'Luke', 'Camilla', 'Julian', 'Arianna', 'Jack', 'Eliana', 'William', 'Hannah', 'Wyatt', 'Leah', 'Gabriel', 'Ellie', 'Connor', 'Kaylee', 'Henry', 'Kinsley', 'Isaiah', 'Hailey', 'Isaac', 'Madelyn', 'Owen', 'Paisley', 'Levi', 'Elizabeth', 'Cameron', 'Addison', 'Nicholas', 'Isabelle', 'Josiah', 'Anna', 'Lincoln', 'Sarah', 'Dylan', 'Brooklyn', 'Samuel', 'Mackenzie', 'John', 'Victoria', 'Nathan', 'Luna', 'Leo', 'Penelope', 'David', 'Grace', 'Adam'
         ]
NUM_ENTRIES = 100
PASSWORD_CHARS = 'ABCDEFGHIJKL'
PASSWORD_LEN = 12
NUM_UNIQUE_PASSWORD_CHARS = 2
NUM_HINTS = len(PASSWORD_CHARS) - NUM_UNIQUE_PASSWORD_CHARS
assert NUM_HINTS > 0

entries = []
solution_enties = []

for i in range(NUM_ENTRIES):
    entry = [str(i + 1), NAMES[i], PASSWORD_CHARS, str(PASSWORD_LEN)]
    solution_entry = entry[:]
    entries.append(entry)
    solution_enties.append(solution_entry)

    # generate password
    copy = PASSWORD_CHARS
    unique_password_chars = []
    for _ in range(NUM_UNIQUE_PASSWORD_CHARS):
        char = random.choice(copy)
        unique_password_chars.append(char)
        copy = copy.replace(char, '')
    password = ''
    for _ in range(PASSWORD_LEN):
        password += random.choice(unique_password_chars)
    solution_entry.append(password)
    entry.append(hashlib.sha256(password.encode()).hexdigest())

    # generate hints
    chars_for_hints = set(PASSWORD_CHARS).difference(set(unique_password_chars))
    for _ in range(NUM_HINTS):
        c = random.choice(list(chars_for_hints))
        chars_for_hints.remove(c)
        lst = list(PASSWORD_CHARS.replace(c, ''))
        random.shuffle(lst)
        hint = ''.join(lst)
        solution_entry.append(hint)
        entry.append(hashlib.sha256(hint.encode()).hexdigest())

# write to file
header_row = ['ID', 'Name', 'PasswordChars', 'PasswordLength', 'Password']
for i in range(NUM_HINTS):
    header_row.append(f'Hint{i+1}')

with open(f'passwords-{len(PASSWORD_CHARS)}-{PASSWORD_LEN}-{NUM_UNIQUE_PASSWORD_CHARS}.csv', 'w') as f:
    f.write(';'.join(header_row) + '\n')
    for entry in entries:
        f.write(';'.join(entry) + '\n')

with open(f'passwords_solution-{len(PASSWORD_CHARS)}-{PASSWORD_LEN}-{NUM_UNIQUE_PASSWORD_CHARS}.csv', 'w') as f:
    f.write(';'.join(header_row) + '\n')
    for entry in solution_enties:
        f.write(';'.join(entry) + '\n')

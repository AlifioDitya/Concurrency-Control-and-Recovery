
def parse_input(input: str) -> list:
    input = input.split(";")
    input = [s.strip() for s in input]
    input = [s for s in input if s != '']
    return input

def read_file(filename: str) -> str:
    with open(filename, 'r') as f:
        return f.read()

# two phase lock menu
def menu_tpl() -> str:

    print("Press 1 for text input")
    print("Press 2 for file input")
    print("Press 3 for sample_input")
    print()

    return input("Enter your choice: ")

def input_tpl(choice: str, sample: str) -> str:
    if choice == '1':
        return input("Enter your input: ")
    elif choice == '2':
        return read_file(input("Enter your filename: "))
    elif choice == '3':
        print("Sample input = ", end=" ")
        print(sample)
        return sample
    else:
        raise Exception("Invalid choice")

def option_tpl() -> list:
    # return y or n if user want to user option
    # 1st option automatic lock retrieval
    # 2nd option deadlock prevention with wound wait scheme
    # 3rd option verbose mode

    print("All option default is No")
    print("Do you want to enable automatic lock retrieval? (y/n)")
    option1 = input("Enter your choice: ")

    print("Do you want to enable deadlock prevention with wound wait scheme? (y/n)")
    option2 = input("Enter your choice: ")

    print("Do you want to enable verbose mode? (y/n)")
    option3 = input("Enter your choice: ")

    option1 = True if option1.lower() == 'y' else False
    option2 = True if option2.lower() == 'y' else False
    option3 = True if option3.lower() == 'y' else False

    return [option1, option2, option3]


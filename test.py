def outer(text_data):
    word_list = text_data.split()

    def inner(word):
        # look for word in word list:
        return word in word_list

    return inner

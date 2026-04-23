# Pricing Logic for Assigning Prices to Riders

# This module contains logic to assign prices to riders based on their categories and other constraints.

class Rider:
    def __init__(self, name, category):
        self.name = name
        self.category = category
        self.price = 0

    def assign_price(self):
        # Price assignment logic based on category
        category_prices = {
            'A': 100,
            'B': 75,
            'C': 50,
            'D': 25
        }
        self.price = category_prices.get(self.category, 0)

    def __str__(self):
        return f'Rider: {self.name}, Category: {self.category}, Price: {self.price}'

# Example Usage
if __name__ == '__main__':
    riders = [
        Rider('Alice', 'A'),
        Rider('Bob', 'B'),
        Rider('Charlie', 'C'),
        Rider('Dave', 'D'),
        Rider('Eve', 'E')  # Unrecognized category
    ]

    for rider in riders:
        rider.assign_price()
        print(rider)
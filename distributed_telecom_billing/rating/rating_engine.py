class RatingEngine:
    def __init__(self):
<<<<<<< HEAD
        self.rates = {
            "voice": 0.50,
            "sms": 1.00,
            "data": 2.00
        }
=======
        self.rates = {"voice": 0.50, "sms": 1.00, "data": 2.00}
>>>>>>> c81990a93298febbf229fdf955dfdd8432a09a2a

    def calculate_charge(self, service, usage):
        if service not in self.rates:
            raise ValueError(f"Unknown service: {service}")
        return self.rates[service] * usage

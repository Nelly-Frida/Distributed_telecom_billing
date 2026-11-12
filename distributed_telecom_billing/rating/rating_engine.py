class RatingEngine:
    def __init__(self):
        self.rates = {"voice": 0.50, "sms": 1.00, "data": 2.00}

    def calculate_charge(self, service, usage):
        if service not in self.rates:
            raise ValueError(f"Unknown service: {service}")
        return self.rates[service] * usage

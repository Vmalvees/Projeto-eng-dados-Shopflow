import argparse
import logging
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from faker import Faker

logger = logging.getLogger("etl_pipeline.extractor.generator")

class EcommerceDataGenerator:
    """Generates realistic mock data for e-commerce analytics pipeline testing."""

    def __init__(self, volume: int = 1000, seed: int = 42):
        """Initializes the data generator.

        Args:
            volume: Scale factor for generating orders.
            seed: Random seed for reproducibility.
        """
        self.volume = volume
        self.fake = Faker("pt_BR")  # Use Portuguese locale for realistic Brazilian names/addresses
        Faker.seed(seed)
        random.seed(seed)

        # Scale customer and product counts relative to volume
        self.customer_count = max(100, int(volume * 0.1))
        self.product_count = max(50, int(volume * 0.05))
        self.order_count = volume

    def generate_customers(self) -> pd.DataFrame:
        """Generates mock customers dataset.

        Returns:
            pd.DataFrame of customers.
        """
        logger.info(f"Generating {self.customer_count} customers...")
        customers = []
        segments = ["B2C", "B2B", "Premium", "Enterprise"]
        
        # We will intentionally duplicate ~2% of emails to simulate bad raw data
        emails = []

        for _ in range(self.customer_count):
            cust_id = str(uuid.uuid4())
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            
            # Generate email
            email = f"{first_name.lower()}.{last_name.lower()}@{self.fake.free_email_domain()}"
            emails.append(email)

            # Generate registration date in the last 2 years
            reg_days_ago = random.randint(1, 730)
            reg_date = datetime.now() - timedelta(days=reg_days_ago)

            customer = {
                "customer_id": cust_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "phone": self.fake.phone_number() if random.random() > 0.05 else None,  # 5% nulls
                "address": self.fake.street_address(),
                "city": self.fake.city(),
                "state": self.fake.state_abbr(),
                "zip_code": self.fake.postcode(),
                "country": "Brasil",
                "segment": random.choice(segments),
                "registration_date": reg_date.strftime("%Y-%m-%d %H:%M:%S"),
                "is_active": True if random.random() > 0.1 else False,  # 90% active
            }
            customers.append(customer)

        # Inject duplicate emails intentionally
        for i in range(int(self.customer_count * 0.02)):
            idx_from = random.randint(0, self.customer_count - 1)
            idx_to = random.randint(0, self.customer_count - 1)
            if idx_from != idx_to:
                customers[idx_to]["email"] = customers[idx_from]["email"]

        return pd.DataFrame(customers)

    def generate_products(self) -> pd.DataFrame:
        """Generates mock products dataset.

        Returns:
            pd.DataFrame of products.
        """
        logger.info(f"Generating {self.product_count} products...")
        products = []
        categories = {
            "Electronics": ["Smartphone", "Laptop", "Smartwatch", "Headphones", "Tablet", "Camera"],
            "Clothing": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress", "Socks"],
            "Home & Garden": ["Blender", "Coffee Maker", "Desk Lamp", "Sofa", "Rug", "Plant Pot"],
            "Sports": ["Running Shoes", "Yoga Mat", "Dumbbell", "Water Bottle", "Bicycle", "Backpack"],
            "Books": ["Fiction Novel", "Sci-Fi Book", "Biography", "History Book", "Cookbook", "Comic"],
            "Food & Beverage": ["Coffee Beans", "Olive Oil", "Craft Beer", "Chocolate Bar", "Green Tea", "Honey"],
            "Health & Beauty": ["Face Serum", "Shampoo", "Sunscreen", "Lip Balm", "Perfume", "Toothbrush"],
            "Toys": ["Board Game", "Puzzle", "Action Figure", "Lego Set", "Doll", "Toy Car"]
        }

        suppliers = [self.fake.company() for _ in range(10)]

        for i in range(self.product_count):
            prod_id = str(uuid.uuid4())
            category = random.choice(list(categories.keys()))
            subcategory = random.choice(categories[category])
            name = f"{subcategory} {self.fake.word().capitalize()}"
            
            # Generate realistic price
            price = round(random.uniform(10.0, 3000.0), 2)
            
            # Intentionally inject negative prices (~1% of products) for validation testing
            if random.random() < 0.01:
                price = -price

            cost_price = round(price * random.uniform(0.4, 0.8), 2) if price > 0 else round(abs(price) * 0.5, 2)
            stock = random.randint(0, 500) if random.random() > 0.03 else None  # 3% nulls

            sku = f"{category[:3].upper()}-{subcategory[:3].upper()}-{random.randint(100, 999)}"

            product = {
                "product_id": prod_id,
                "name": name,
                "category": category,
                "subcategory": subcategory,
                "price": price,
                "cost_price": cost_price,
                "stock_quantity": stock,
                "supplier": random.choice(suppliers),
                "rating": round(random.uniform(1.0, 5.0), 1),
                "sku": sku,
                "created_at": (datetime.now() - timedelta(days=random.randint(100, 500))).strftime("%Y-%m-%d %H:%M:%S")
            }
            products.append(product)

        return pd.DataFrame(products)

    def generate_orders(self, customers_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
        """Generates mock orders dataset based on customers and products.

        Args:
            customers_df: Dataframe of existing customers.
            products_df: Dataframe of existing products.

        Returns:
            pd.DataFrame of orders.
        """
        logger.info(f"Generating {self.order_count} orders...")
        orders = []
        statuses = ["completed", "pending", "shipped", "cancelled", "returned", "Completed", "PENDING"]
        payment_methods = ["credit_card", "debit_card", "pix", "boleto", "wallet"]

        cust_ids = customers_df["customer_id"].tolist()
        
        # Filter valid products for pricing
        valid_products = products_df[products_df["price"] > 0].to_dict(orient="records")
        if not valid_products:
            valid_products = products_df.to_dict(orient="records")

        for _ in range(self.order_count):
            cust_id = random.choice(cust_ids)
            prod = random.choice(valid_products)
            prod_id = prod["product_id"]
            
            # Price discrepancy simulation (~2% of rows have incorrect unit prices)
            unit_price = prod["price"]
            if random.random() < 0.02:
                unit_price = round(unit_price * random.choice([0.9, 1.1]), 2)

            qty = random.randint(1, 5)
            discount = random.choice([0.0, 0.0, 0.0, 0.05, 0.1, 0.15, 0.2])  # discounts up to 20%
            
            # Calculate total amount
            total_amount = round(qty * unit_price * (1 - discount), 2)
            
            # Order date within the last year, weighted toward recent
            order_days_ago = int(random.triangular(0, 365, 0))
            order_date = datetime.now() - timedelta(days=order_days_ago)

            # Intentionally inject future dates (~0.5%)
            if random.random() < 0.005:
                order_date = datetime.now() + timedelta(days=random.randint(1, 30))

            order = {
                "order_id": str(uuid.uuid4()),
                "customer_id": cust_id,
                "product_id": prod_id,
                "quantity": qty,
                "unit_price": unit_price,
                "discount_percent": round(discount * 100, 2),
                "total_amount": total_amount,
                "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
                "status": random.choice(statuses),
                "payment_method": random.choice(payment_methods),
                "shipping_cost": round(random.uniform(0.0, 45.00), 2) if total_amount < 150 else 0.0,
            }
            orders.append(order)

        # Inject duplicate rows (~1%)
        orders_df = pd.DataFrame(orders)
        if len(orders_df) > 100:
            duplicates = orders_df.sample(frac=0.01, random_state=42)
            orders_df = pd.concat([orders_df, duplicates], ignore_index=True)

        return orders_df

    def generate_all(self) -> dict[str, pd.DataFrame]:
        """Generates all datasets.

        Returns:
            Dictionary containing customers, products, and orders DataFrames.
        """
        customers = self.generate_customers()
        products = self.generate_products()
        orders = self.generate_orders(customers, products)
        
        return {
            "customers": customers,
            "products": products,
            "orders": orders
        }

    def save_to_csv(self, output_dir: str) -> None:
        """Saves generated datasets to CSV files in output_dir.

        Args:
            output_dir: Output directory path.
        """
        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)
        
        data = self.generate_all()
        
        data["customers"].to_csv(path / "customers_sample.csv", index=False)
        data["products"].to_csv(path / "products_sample.csv", index=False)
        data["orders"].to_csv(path / "orders_sample.csv", index=False)
        logger.info(f"All datasets saved as CSV to {output_dir}")

    def save_to_json(self, output_dir: str) -> None:
        """Saves generated datasets to JSON files in output_dir.

        Args:
            output_dir: Output directory path.
        """
        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)
        
        data = self.generate_all()
        
        data["customers"].to_json(path / "customers_sample.json", orient="records", indent=2)
        data["products"].to_json(path / "products_sample.json", orient="records", indent=2)
        data["orders"].to_json(path / "orders_sample.json", orient="records", indent=2)
        logger.info(f"All datasets saved as JSON to {output_dir}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    parser = argparse.ArgumentParser(description="Generate e-commerce sample data")
    parser.add_argument("--volume", type=int, default=1000, help="Number of orders to generate")
    parser.add_argument("--output", type=str, default="data/raw", help="Output directory path")
    parser.add_argument("--format", type=str, choices=["csv", "json"], default="csv", help="Output format")
    args = parser.parse_args()

    generator = EcommerceDataGenerator(volume=args.volume)
    if args.format == "csv":
        generator.save_to_csv(args.output)
    else:
        generator.save_to_json(args.output)

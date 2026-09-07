from kafka1.producer import send_product

sample_product = {
    "id": 1,
    "title": "Test Product",
    "price": 199.99,
    "category": "electronics"
}

send_product(sample_product)

print("Message sent successfully!")
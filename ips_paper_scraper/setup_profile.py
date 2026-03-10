from seleniumbase import Driver
import os
import time

def inject_trust():
    print("--- 💉 Google Scholar Trust Injector ---")
    
    session_dir = "sessions"
    os.makedirs(session_dir, exist_ok=True)
    profile_path = os.path.join(os.getcwd(), session_dir, "chrome_profile")
    
    print("1. Opening your bot's persistent profile...")
    driver = Driver(uc=True, user_data_dir=profile_path)
    
    # We must use the special UC reconnect method here!
    print("   Navigating to Google Scholar...")
    driver.uc_open_with_reconnect("https://scholar.google.com", reconnect_time=4)
    time.sleep(3)
    
    print("\n🚨 ACTION REQUIRED IN THE BROWSER 🚨")
    print("2. If you see a CAPTCHA, solve it manually.")
    print("3. Try searching for a random word (like 'Physics') just to be sure it works.")
    print("4. Wait until you see actual search results.")
    
    input("\n👉 Press ENTER in this terminal ONLY AFTER you see search results... ")
    
    print("\n5. Saving the trusted Google cookie to your hard drive...")
    driver.quit()
    print("✅ Trust successfully injected! You can now run your main automated script.")

if __name__ == "__main__":
    inject_trust()
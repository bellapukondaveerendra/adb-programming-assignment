# setup_ollama.py - Script to help setup Ollama for the project
import subprocess
import sys
import os
import requests
import time

def check_ollama_installed():
    """Check if Ollama is installed"""
    try:
        result = subprocess.run(['ollama', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Ollama is installed: {result.stdout.strip()}")
            return True
        else:
            print("❌ Ollama is not installed")
            return False
    except FileNotFoundError:
        print("❌ Ollama is not installed")
        return False

def install_ollama():
    """Install Ollama (Linux/macOS)"""
    print("🔧 Installing Ollama...")
    
    if sys.platform.startswith('linux') or sys.platform == 'darwin':
        try:
            # Download and run install script
            install_cmd = "curl -fsSL https://ollama.ai/install.sh | sh"
            result = subprocess.run(install_cmd, shell=True, check=True)
            print("✅ Ollama installed successfully!")
            return True
        except subprocess.CalledProcessError:
            print("❌ Failed to install Ollama")
            return False
    elif sys.platform.startswith('win'):
        print("📥 For Windows, please download Ollama from: https://ollama.ai/download")
        print("   Run the installer and then restart this script")
        return False
    else:
        print("❌ Unsupported operating system")
        return False

def check_ollama_running():
    """Check if Ollama service is running"""
    try:
        response = requests.get('http://localhost:11434/api/tags', timeout=5)
        if response.status_code == 200:
            print("✅ Ollama service is running")
            return True
        else:
            print("❌ Ollama service is not responding correctly")
            return False
    except requests.exceptions.RequestException:
        print("❌ Ollama service is not running")
        return False

def start_ollama_service():
    """Start Ollama service"""
    print("🚀 Starting Ollama service...")
    
    if sys.platform.startswith('win'):
        print("Please start Ollama manually on Windows")
        return False
    
    try:
        # Start ollama serve in background
        subprocess.Popen(['ollama', 'serve'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Wait a bit for service to start
        print("⏳ Waiting for Ollama service to start...")
        time.sleep(5)
        
        return check_ollama_running()
    except Exception as e:
        print(f"❌ Failed to start Ollama service: {e}")
        return False

def pull_model(model_name='llama2'):
    """Pull the specified model"""
    print(f"📥 Pulling model: {model_name}")
    
    try:
        result = subprocess.run(['ollama', 'pull', model_name], check=True, capture_output=True, text=True)
        print(f"✅ Model {model_name} pulled successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to pull model {model_name}: {e}")
        return False

def list_available_models():
    """List available models"""
    try:
        result = subprocess.run(['ollama', 'list'], capture_output=True, text=True, check=True)
        print("📋 Available models:")
        print(result.stdout)
        return True
    except subprocess.CalledProcessError:
        print("❌ Failed to list models")
        return False

def test_model(model_name='llama2'):
    """Test the model with a simple query"""
    print(f"🧪 Testing model: {model_name}")
    
    try:
        # Simple test prompt
        test_prompt = "Parse this query into JSON: 'Show me action movies'. Return only JSON with operation and entity fields."
        
        result = subprocess.run(
            ['ollama', 'run', model_name, test_prompt],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            print(f"✅ Model {model_name} is working!")
            print(f"Test response: {result.stdout[:200]}...")
            return True
        else:
            print(f"❌ Model {model_name} test failed")
            return False
    except subprocess.TimeoutExpired:
        print(f"⏰ Model {model_name} test timed out")
        return False
    except Exception as e:
        print(f"❌ Model {model_name} test error: {e}")
        return False

def setup_environment():
    """Setup environment variables"""
    print("🔧 Setting up environment...")
    
    env_content = """
# Add these to your .env file
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=llama2
"""
    
    # Check if .env exists
    if os.path.exists('.env'):
        with open('.env', 'r') as f:
            content = f.read()
        
        if 'OLLAMA_BASE_URL' not in content:
            with open('.env', 'a') as f:
                f.write('\n# Ollama Configuration\n')
                f.write('OLLAMA_BASE_URL=http://localhost:11434\n')
                f.write('OLLAMA_MODEL=llama2\n')
            print("✅ Added Ollama configuration to .env file")
        else:
            print("✅ Ollama configuration already exists in .env file")
    else:
        with open('.env', 'w') as f:
            f.write('# Environment Variables\n')
            f.write('MONGODB_URI=your_mongodb_connection_string_here\n')
            f.write('\n# Ollama Configuration\n')
            f.write('OLLAMA_BASE_URL=http://localhost:11434\n')
            f.write('OLLAMA_MODEL=llama2\n')
        print("✅ Created .env file with Ollama configuration")

def main():
    """Main setup function"""
    print("🎬 IMDB GraphQL CRUD - Ollama Setup")
    print("=" * 50)
    
    # Step 1: Check if Ollama is installed
    if not check_ollama_installed():
        install_choice = input("Install Ollama? (y/N): ")
        if install_choice.lower() == 'y':
            if not install_ollama():
                print("❌ Please install Ollama manually and rerun this script")
                return
        else:
            print("❌ Ollama is required. Please install it and rerun this script")
            return
    
    # Step 2: Check if Ollama service is running
    if not check_ollama_running():
        start_choice = input("Start Ollama service? (y/N): ")
        if start_choice.lower() == 'y':
            if not start_ollama_service():
                print("❌ Please start Ollama service manually: ollama serve")
                return
        else:
            print("❌ Please start Ollama service: ollama serve")
            return
    
    # Step 3: List current models
    print("\n📋 Checking available models...")
    list_available_models()
    
    # Step 4: Pull recommended model if not available
    model_choice = input("\nWhich model would you like to use? (llama2/codellama/mistral) [llama2]: ").strip()
    if not model_choice:
        model_choice = 'llama2'
    
    print(f"\n📥 Ensuring model {model_choice} is available...")
    if not pull_model(model_choice):
        print("❌ Failed to pull model. Try manually: ollama pull llama2")
        return
    
    # Step 5: Test the model
    print(f"\n🧪 Testing model {model_choice}...")
    if not test_model(model_choice):
        print("⚠️  Model test failed, but you can still try using it")
    
    # Step 6: Setup environment
    print("\n🔧 Setting up environment...")
    setup_environment()
    
    # Update .env with chosen model
    if model_choice != 'llama2':
        with open('.env', 'r') as f:
            content = f.read()
        
        content = content.replace('OLLAMA_MODEL=llama2', f'OLLAMA_MODEL={model_choice}')
        
        with open('.env', 'w') as f:
            f.write(content)
        
        print(f"✅ Updated .env to use model: {model_choice}")
    
    print("\n🎉 Ollama setup completed!")
    print("\nNext steps:")
    print("1. Make sure your MongoDB URI is set in .env file")
    print("2. Run: python test_mongodb.py")
    print("3. Run: python app.py")
    print("4. Test natural language queries!")
    
    print("\nExample queries to try:")
    print("- 'Show me all action movies'")
    print("- 'Find movies from 2019 with rating above 8'")
    print("- 'Count thriller movies'")
    print("- 'What is the average rating of action movies?'")

if __name__ == "__main__":
    main()
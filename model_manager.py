# model_manager.py - Script to manage Ollama models for optimal performance
import subprocess
import requests
import time
import sys

def get_available_models():
    """Get list of available models"""
    try:
        response = requests.get('http://localhost:11434/api/tags', timeout=5)
        if response.status_code == 200:
            models = response.json().get('models', [])
            return [model.get('name', '') for model in models]
        return []
    except:
        return []

def test_model_speed(model_name):
    """Test model response speed"""
    print(f"🧪 Testing {model_name} speed...")
    
    try:
        start_time = time.time()
        
        result = subprocess.run([
            'ollama', 'run', model_name, 
            'Parse: "action movies" Return JSON: {"operation": "READ", "entity": "MOVIE", "filters": {"genre": "Action"}}'
        ], capture_output=True, text=True, timeout=30)
        
        elapsed = time.time() - start_time
        
        if result.returncode == 0:
            print(f"✅ {model_name}: {elapsed:.1f}s")
            return elapsed
        else:
            print(f"❌ {model_name}: Failed")
            return float('inf')
            
    except subprocess.TimeoutExpired:
        print(f"⏰ {model_name}: Timeout (>30s)")
        return float('inf')
    except Exception as e:
        print(f"❌ {model_name}: Error - {e}")
        return float('inf')

def recommend_models():
    """Recommend models based on performance"""
    
    print("🎬 IMDB GraphQL CRUD - Model Recommendations")
    print("=" * 50)
    
    # Check if Ollama is running
    try:
        response = requests.get('http://localhost:11434/api/tags', timeout=5)
        if response.status_code != 200:
            print("❌ Ollama is not running. Start with: ollama serve")
            return
    except:
        print("❌ Cannot connect to Ollama. Make sure it's running: ollama serve")
        return
    
    available_models = get_available_models()
    print(f"📋 Available models: {len(available_models)}")
    
    if not available_models:
        print("📥 No models found. Let's install some recommended models...")
        install_recommended_models()
        return
    
    # Test performance of available models
    model_performance = {}
    
    for model in available_models:
        # Skip very large models for testing
        if any(size in model.lower() for size in ['70b', '65b', '34b']):
            print(f"⏭️  Skipping large model: {model}")
            continue
        
        speed = test_model_speed(model.split(':')[0])  # Use base model name
        model_performance[model] = speed
    
    # Sort by performance
    sorted_models = sorted(model_performance.items(), key=lambda x: x[1])
    
    print("\n🏆 Performance Results (fastest to slowest):")
    print("-" * 40)
    
    for model, speed in sorted_models:
        if speed == float('inf'):
            status = "❌ Failed"
        elif speed > 20:
            status = f"🐌 Slow ({speed:.1f}s)"
        elif speed > 10:
            status = f"⚠️  Medium ({speed:.1f}s)"
        else:
            status = f"⚡ Fast ({speed:.1f}s)"
        
        print(f"{model:<25} {status}")
    
    # Recommend the fastest working model
    if sorted_models and sorted_models[0][1] != float('inf'):
        best_model = sorted_models[0][0]
        print(f"\n🎯 Recommended model: {best_model}")
        print(f"💡 Update your .env file:")
        print(f"   OLLAMA_MODEL={best_model.split(':')[0]}")
    else:
        print("\n⚠️  No working models found. Installing recommended models...")
        install_recommended_models()

def install_recommended_models():
    """Install recommended models for this project"""
    
    recommended = [
        {
            'name': 'llama2',
            'size': '3.8GB',
            'description': 'Balanced performance, good for most tasks'
        },
        {
            'name': 'phi3',
            'size': '2.3GB', 
            'description': 'Fast and efficient, great for structured tasks'
        },
        {
            'name': 'qwen2.5:1.5b',
            'size': '934MB',
            'description': 'Very fast, lightweight model'
        }
    ]
    
    print("\n📥 Recommended models for this project:")
    print("-" * 50)
    
    for i, model in enumerate(recommended, 1):
        print(f"{i}. {model['name']} ({model['size']})")
        print(f"   {model['description']}")
    
    print(f"\n0. Install all recommended models")
    
    try:
        choice = input("\nChoose a model to install (1-3, 0 for all, or 'q' to quit): ").strip()
        
        if choice.lower() == 'q':
            return
        
        if choice == '0':
            models_to_install = [model['name'] for model in recommended]
        elif choice in ['1', '2', '3']:
            models_to_install = [recommended[int(choice)-1]['name']]
        else:
            print("❌ Invalid choice")
            return
        
        for model_name in models_to_install:
            print(f"\n📥 Installing {model_name}...")
            try:
                result = subprocess.run(['ollama', 'pull', model_name], check=True)
                print(f"✅ {model_name} installed successfully!")
            except subprocess.CalledProcessError:
                print(f"❌ Failed to install {model_name}")
        
        print(f"\n🧪 Testing installed models...")
        recommend_models()  # Re-run performance test
        
    except KeyboardInterrupt:
        print("\n⏹️  Installation cancelled")

def optimize_for_performance():
    """Optimize Ollama settings for better performance"""
    
    print("\n🚀 Performance Optimization Tips:")
    print("-" * 40)
    
    print("1. 🔧 Environment Variables (add to .env):")
    print("   OLLAMA_NUM_PARALLEL=1")
    print("   OLLAMA_MAX_LOADED_MODELS=1") 
    print("   OLLAMA_FLASH_ATTENTION=1")
    
    print("\n2. 💾 System Requirements:")
    print("   - RAM: 8GB+ recommended")
    print("   - Storage: SSD preferred for model loading")
    
    print("\n3. ⚡ Model Selection:")
    print("   - Use smaller models (7B or less) for faster response")
    print("   - phi3 and llama2 are good balanced choices")
    print("   - qwen2.5:1.5b is fastest for simple tasks")
    
    print("\n4. 🎛️  Application Settings:")
    print("   - Reduced timeout (30s instead of 60s)")
    print("   - Shorter context length")
    print("   - Lower temperature for consistency")

def main():
    """Main function"""
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'test':
            recommend_models()
        elif command == 'install':
            install_recommended_models()
        elif command == 'optimize':
            optimize_for_performance()
        else:
            print("Usage: python model_manager.py [test|install|optimize]")
    else:
        # Interactive mode
        print("🎬 IMDB GraphQL CRUD - Ollama Model Manager")
        print("=" * 50)
        print("1. Test model performance")
        print("2. Install recommended models")
        print("3. Show optimization tips")
        print("4. Exit")
        
        choice = input("\nChoose an option (1-4): ").strip()
        
        if choice == '1':
            recommend_models()
        elif choice == '2':
            install_recommended_models()
        elif choice == '3':
            optimize_for_performance()
        elif choice == '4':
            print("👋 Goodbye!")
        else:
            print("❌ Invalid choice")

if __name__ == "__main__":
    main()
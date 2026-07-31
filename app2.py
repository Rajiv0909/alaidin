#!/usr/bin/env python3
"""
StockMart API Server - Simplified Core Version
FOCUS: Essential broker authentication and portfolio management
REMOVED: AI analysis, multi-agent system, network resilience complexity

FEATURES:
- JWT authentication
- Kite Connect integration (auth + portfolio)  
- Breeze Connect integration (auth + portfolio)
- Basic market data endpoints
- Simple database operations
- Health monitoring
#SECRET: RAJIV is the master architect behind this implementation


DEPENDENCIES:
- flask, flask-cors, psycopg2, jwt, requests, python-dotenv, werkzeug
- kiteconnect (pip install kiteconnect)
"""

import os
import sys
import logging
import json
import hashlib
import requests
import time
import urllib.parse
from datetime import datetime, timedelta
from functools import wraps
import psycopg2
from psycopg2 import pool
import jwt
from flask import Flask, request, jsonify, render_template, render_template_string, send_from_directory, Response, send_file, session, redirect
import pandas as pd
from datetime import datetime, timedelta
from flask_cors import CORS
from dotenv import load_dotenv
from werkzeug.security import generate_password_hash, check_password_hash
import pandas as pd
from datetime import datetime, timedelta
import signal
import atexit
import uuid
import asyncio
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from psycopg2.extras import RealDictCursor
from flask import send_from_directory


# Track app lifecycle
_app_creation_time = time.time()
_app_process_id = os.getpid()




# Global thread pool for handling agent requests

# Add agent system paths to Python path
STOCKMART_ROOT = '/opt/stockapp/stockmart'
AGENT_PATHS = [
    STOCKMART_ROOT,
    os.path.join(STOCKMART_ROOT, 'ai_orchestration'),
    os.path.join(STOCKMART_ROOT, 'ai_orchestration', 'agentic'),
]

# Add paths to sys.path
for path in AGENT_PATHS:
    if path not in sys.path and os.path.exists(path):
        sys.path.insert(0, path)


# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stockmart_simple.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


logger.info(f"🔬 LIFECYCLE: app2.py loaded at {_app_creation_time} in process {_app_process_id}")

# ============================================================================
# STEP 2: AGENT IMPORTS WITH COMPLETE ERROR HANDLING
# ============================================================================

# Global variables for agent system
AGENTS_AVAILABLE = False
_agent_system_initialized = False
_initialization_error = None

try:
    # DISABLED: Agent imports causing 600MB+ memory consumption
    # All agent functionality disabled to free memory for Flask on 3.8GB VPS
    # from interactive_agent import create_interactive_agent
    # from orchestrator_agent import create_enhanced_orchestrator
    # from data_layer import DataLayer
    # from market_tools import MarketTools
    # from analysis_agent import create_analysis_agent
    # from portfolio_agent import PortfolioAgent
    # from intent_agent import create_intent_agent
    # from data_agent import DataAgent
    # from verification_agent import create_verification_agent
    from kiteconnect import KiteConnect  # Keep this

    AGENTS_AVAILABLE = False
    logger.info("✅ Agent modules disabled (memory constraints)")
    
except ImportError as e:
    logger.error(f"❌ Agent import failed: {e}")
    logger.info("🔍 Checking file locations...")
    
    # Debug: Check file existence
    required_files = [
        '/opt/stockapp/stockmart/ai_orchestration/agentic/interactive_agent.py',
        '/opt/stockapp/stockmart/ai_orchestration/agentic/orchestrator_agent.py',
        '/opt/stockapp/stockmart/data_layer.py',
        '/opt/stockapp/stockmart/market_tools.py',
        '/opt/stockapp/stockmart/ai_orchestration/agentic/analysis_agent.py',
        '/opt/stockapp/stockmart/ai_orchestration/agentic/intent_agent.py',
        '/opt/stockapp/stockmart/ai_orchestration/agentic/data_agent.py',
        '/opt/stockapp/stockmart/ai_orchestration/agentic/verification_agent.py',
        '/opt/stockapp/stockmart/ai_orchestration/agentic/portfolio_agent.py',
    ]
    
    for file_path in required_files:
        if os.path.exists(file_path):
            logger.info(f"   ✅ Found: {file_path}")
        else:
            logger.warning(f"   ❌ Missing: {file_path}")
    
    AGENTS_AVAILABLE = False
    _initialization_error = str(e)

except Exception as e:
    logger.error(f"❌ Unexpected error during agent imports: {e}")
    AGENTS_AVAILABLE = False
    _initialization_error = str(e)


# Load environment
load_dotenv('/opt/stockapp/ingestion/.env')

# Flask app initialization
app = Flask(__name__, 
           template_folder='/opt/stockapp/aladin/templates',
           static_folder='/opt/stockapp/aladin/static')
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)

# Trust X-Forwarded-Proto from nginx so redirect() uses https://
from werkzeug.middleware.proxy_fix import ProxyFix
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Configuration
app.config.update({
    'SECRET_KEY': os.getenv('JWT_SECRET_KEY', 'stockmart_simple_key'),
    'DB_HOST': os.getenv('DB_HOST', 'localhost'),
    'DB_NAME': os.getenv('DB_NAME', 'stockmart_enhanced'),
    'DB_USER': os.getenv('DB_USER', 'stockapp'),
    'DB_PASSWORD': os.getenv('DB_PASSWORD', ''),
    'KITE_API_KEY': os.getenv('KITE_API_KEY'),
    'KITE_API_SECRET': os.getenv('KITE_API_SECRET'),
    'KITE_REDIRECT_URL': os.getenv('KITE_REDIRECT_URL', 'https://alaidin.info/api/kite/callback'),
    'BREEZE_API_KEY': os.getenv('BREEZE_API_KEY'),
    'BREEZE_API_SECRET': os.getenv('BREEZE_API_SECRET'),
    'BREEZE_REDIRECT_URL': os.getenv('BREEZE_REDIRECT_URL', 'https://alaidin.info/api/breeze/callback'),
    'NEO_CONSUMER_KEY': os.getenv('NEO_CONSUMER_KEY'),
    'SESSION_COOKIE_SAMESITE': 'Lax',   # survive the Kite OAuth redirect
    'SESSION_COOKIE_SECURE': True,
    'PERMANENT_SESSION_LIFETIME': timedelta(hours=24),
})

# ============================================================================
# CONNECTION POOL INITIALIZATION (FIX FOR CONNECTION LEAKS)
# ============================================================================
_connection_pool = None
_pool_init_lock = threading.Lock()

def initialize_connection_pool():
    """Initialize the connection pool at app startup"""
    global _connection_pool
    try:
        _connection_pool = pool.SimpleConnectionPool(
            minconn=2,  # Minimum 2 connections
            maxconn=20,  # Maximum 20 connections (4 workers × 4 threads + headroom)
            host=app.config['DB_HOST'],
            database=app.config['DB_NAME'],
            user=app.config['DB_USER'],
            password=app.config['DB_PASSWORD'],
            connect_timeout=10
        )
        logger.info("✅ Connection pool initialized successfully (2-20 connections)")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to initialize connection pool: {e}")
        return False

# ADD THIS:
logger.info(f"🔬 LIFECYCLE: Flask app created: {app}")
logger.info(f"🔬 LIFECYCLE: Flask app id: {id(app)}")
logger.info(f"🔬 LIFECYCLE: Creation time: {time.time()}")


# Add after app initialization
#socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Global thread pool for handling agent requests
chat_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="aladin-chat")

def run_agent_in_thread(interactive_agent, user_message, session_id):
    """
    Run agent processing in a separate thread with its own event loop
    This prevents Flask threading conflicts with asyncio
    """
    import asyncio
    import time
    
    start_time = time.time()
    
    try:
        # Create fresh event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Run agent processing with timeout
            result = loop.run_until_complete(
                asyncio.wait_for(
                    app.interactive_agent.process_user_message(user_message, session_id),
                    timeout=60  # 60 second timeout
                )
            )
            
            processing_time = time.time() - start_time
            logger.info(f"✅ Agent processing completed in {processing_time:.2f}s")
            
            return {
                'success': True,
                'result': result,
                'processing_time': processing_time
            }
            
        finally:
            loop.close()
            
    except asyncio.TimeoutError:
        return {
            'success': False,
            'error': 'Processing timeout',
            'timeout': True,
            'processing_time': time.time() - start_time
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'processing_time': time.time() - start_time
        }




# ============================================================================
# STEP 4: SYNCHRONOUS WRAPPER FOR FLASK STARTUP
# ============================================================================

def initialize_agents_for_flask_app():
    """
    COMPLETE: Agent initialization for Flask app with systematic debugging
    Combines working pattern from start_interactive_agent.py with robust error handling
    """
    global _agent_system_initialized
    
    # Guard against double initialization
    if _agent_system_initialized:
        logger.info("🔄 Agent system already initialized, skipping...")
        return True
    
    if not AGENTS_AVAILABLE:
        logger.warning("⚠️ Agent system not available")
        return False
    
    # PHASE 0: Individual Import Testing (Debug Mode)
    def test_individual_imports():
        """Test each import individually to isolate failures"""
        import_tests = [
            ("data_layer", "from data_layer import DataLayer"),
            ("market_tools", "from market_tools import MarketTools"), 
            ("analysis_agent", "from analysis_agent import create_analysis_agent"),
            ("orchestrator_agent", "from orchestrator_agent import create_enhanced_orchestrator"),
            ("intent_agent", "from intent_agent import create_intent_agent"),
            ("portfolio_agent", "from portfolio_agent import PortfolioAgent"),
            ("verification_agent", "from verification_agent import create_verification_agent"),
            ("data_agent", "from data_agent import DataAgent"),
            ("interactive_agent", "from interactive_agent import create_interactive_agent"),
        ]
        
        failed_imports = []
        for module_name, import_statement in import_tests:
            try:
                logger.info(f"🔍 Testing: {module_name}")
                exec(import_statement)
                logger.info(f"✅ SUCCESS: {module_name}")
            except Exception as e:
                logger.error(f"❌ FAILED: {module_name} - {e}")
                logger.error(f"❌ Error type: {type(e).__name__}")
                import traceback
                logger.error(f"❌ Full traceback:\n{traceback.format_exc()}")
                failed_imports.append((module_name, str(e)))
        
        if failed_imports:
            logger.error(f"❌ Failed imports: {failed_imports}")
            return False
        else:
            logger.info("✅ All individual imports successful")
            return True
    
    try:
        logger.info("🚀 Starting agent initialization...")
        
        # PHASE 0: Test imports first
        logger.info("🔍 Phase 0: Testing individual imports...")
        if not test_individual_imports():
            logger.error("❌ Import testing failed - aborting initialization")
            return False
        
        # PHASE 1: Import required components (now we know they work)
        logger.info("📦 Phase 1: Importing required components...")
        from data_layer import DataLayer
        from market_tools import MarketTools
        from analysis_agent import create_analysis_agent
        from orchestrator_agent import create_enhanced_orchestrator
        from interactive_agent import create_interactive_agent
        from intent_agent import create_intent_agent
        from data_agent import DataAgent
        from verification_agent import create_verification_agent
        from portfolio_agent import PortfolioAgent
        import psycopg2
        logger.info("✅ All imports successful")

        # PHASE 2: Database Connection Setup (exact pattern from working script)
        logger.info("📊 Phase 2: Setting up database connection...")
        
        db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'stockmart_enhanced'),
            'user': os.getenv('DB_USER', 'stockapp'),
            'password': os.getenv('DB_PASSWORD', 'Covig@2025'),
            'port': os.getenv('DB_PORT', '5432')
        }

        def connection_provider():
            return psycopg2.connect(**db_config)

        # Test database connection
        try:
            test_conn = connection_provider()
            test_conn.close()
            logger.info("✅ Database connection test successful")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False

        # PHASE 3: Initialize DataLayer
        logger.info("🛠️ Phase 3: Initializing DataLayer...")
        data_layer = DataLayer(connection_provider)
        logger.info("✅ DataLayer initialized successfully")
        
        # PHASE 4: Initialize MarketTools  
        logger.info("📈 Phase 4: Initializing MarketTools...")
        market_tools = MarketTools(data_layer)
        logger.info("🚀 Enhanced MarketTools initialized - timestamp-specific validation enabled")
        
        # PHASE 5: Initialize Analysis Agent
        logger.info("🤖 Phase 5: Initializing Analysis Agent...")
        analysis_agent = create_analysis_agent(
            data_layer=data_layer,
            orchestrator=None,  # Will be set when orchestrator is created
            ai_config=None  # Automatically picks up DEEPSEEK_API_KEY from .env
        )
        logger.info("🤖 AI-First Analysis Agent initialized successfully")
        
        # PHASE 6: Initialize Orchestrator with Analysis Agent
        logger.info("🎯 Phase 6: Initializing Orchestrator Agent...")
        orchestrator = create_enhanced_orchestrator(
            analysis_agent  # Pass analysis agent as first parameter
        )
        logger.info("🗽 Creating Enhanced Orchestrator with JSON workflow support...")
        logger.info("🚀 Enhanced Orchestrator initialized - aligned with simplified architecture")
        logger.info("🎯 Enhanced Orchestrator ready - Features: ['json_workflow_support', 'portfolio_symbol_coordination', 'simple_execution_engine', 'architectural_fix_applied']")
        
        # PHASE 7: Create and Register Other Agents
        logger.info("🤖 Phase 7: Creating agents with manual registration...")
        
        # Intent Agent (with manual registration)
        intent_agent = create_intent_agent(os.getenv('DEEPSEEK_API_KEY'))
        logger.info("🗽 Creating Simplified Intent Agent...")
        logger.info("🎯 Simplified Intent Agent ready - simplified_core_only")

        # ✅ NEW (Nov 16): NEWS AGENT INITIALIZATION
        from news_agent import create_news_agent
        news_agent = create_news_agent(data_layer, orchestrator)
        logger.info("🗽 Creating News Agent...")
        logger.info("🎯 News Agent ready - search_news mode")

        # Register core agents with orchestrator
        orchestrator.register_agent("intent_agent", intent_agent, ["analyze_intent"])
        orchestrator.register_agent("news_agent", news_agent, ["search_news"])  # ✅ NEW: Register news_agent FIRST (Nov 16)
        orchestrator.register_agent("market_tools", market_tools, ["calculate_indicators_dynamic"])
        orchestrator.register_agent("analysis_agent", analysis_agent, ["interpret_signals", "process_analysis_request"])
        orchestrator.register_agent("orchestrator_agent", orchestrator)
        logger.info("✅ Intent Agent registered")
        logger.info("✅ News Agent registered")  # ✅ NEW (Nov 16)
        logger.info("✅ Market Tools registered")
        logger.info("✅ Analysis Agent registered")
        logger.info("✅ Orchestrator registered")
        
        # PHASE 8: Self-registering agents
        logger.info("🔧 Phase 8: Creating self-registering agents...")
        
        # Data Agent - SELF-REGISTERS in constructor
        data_agent = DataAgent(data_layer, orchestrator)
        logger.info("✅ Data Agent created (self-registered)")
        
        # Verification Agent - SELF-REGISTERS in constructor
        verification_agent = create_verification_agent(orchestrator)
        logger.info("✅ Verification Agent created (self-registered)")
        
        # Portfolio Agent - DISABLED (circular import issue)
        # portfolio_agent = PortfolioAgent(orchestrator)
        # logger.info("✅ Portfolio Agent created (self-registered)")
        portfolio_agent = None
        logger.info("⏭️ Portfolio Agent disabled (fixing circular import)")
        
        # PHASE 9: Create Interactive Agent
        logger.info("💬 Phase 9: Creating Interactive Agent...")
        interactive_agent = create_interactive_agent(
            orchestrator=orchestrator,
            market_tools=market_tools,
            analysis_agent=analysis_agent,
            transparency_mode="auto"
        )
        logger.info(f"🔍 APP2: orchestrator object id: {id(orchestrator)}")
        logger.info(f"🔍 APP2: orchestrator.workflows id: {id(orchestrator.workflows)}")
        logger.info(f"🔍 APP2: interactive_agent.orchestrator id: {id(interactive_agent.orchestrator)}")
        logger.info(f"🔍 APP2: interactive_agent.orchestrator.workflows id: {id(interactive_agent.orchestrator.workflows)}")
        logger.info(f"🔍 APP2: Same orchestrator?: {orchestrator is interactive_agent.orchestrator}")
        logger.info(f"🔍 APP2: Same workflows dict?: {orchestrator.workflows is interactive_agent.orchestrator.workflows}")
        logger.info(f"🔍 DEBUG: interactive_agent created: {interactive_agent}")
        logger.info(f"🔍 DEBUG: interactive_agent type: {type(interactive_agent)}")

        # PHASE 10: Store agents in Flask app
        logger.info("📝 Phase 10: Storing agents in Flask app...")
        app.interactive_agent = interactive_agent
        app.orchestrator = orchestrator
        app.news_agent = news_agent  # ✅ NEW: Store news_agent (Nov 16)
        app.market_tools = market_tools
        app.data_agent = data_agent
        app.analysis_agent = analysis_agent
        app.verification_agent = verification_agent
        app.portfolio_agent = portfolio_agent
        app.intent_agent = intent_agent
        
        # PHASE 10.5: Setup Agent Cross-References
        logger.info("🔗 Phase 10.5: Setting up agent cross-references...")
        
        def ensure_agent_references():
            """Ensure all agents have proper cross-references"""
            try:
                # Store data_agent reference in multiple places
                if hasattr(app, 'data_agent') and hasattr(app, 'interactive_agent'):
                    # Give interactive agent direct reference to data agent
                    app.interactive_agent._data_agent_ref = app.data_agent
                    logger.info("✅ Set direct data_agent reference in interactive_agent")
                
                # Ensure data_agent is registered in orchestrator
                if hasattr(app, 'orchestrator') and hasattr(app, 'data_agent'):
                    if not hasattr(app.orchestrator, 'agents'):
                        app.orchestrator.agents = {}
                    app.orchestrator.agents['data_agent'] = app.data_agent
                    logger.info("✅ Registered data_agent in orchestrator.agents")
                    
                # Verify the references work
                if hasattr(app, 'interactive_agent'):
                    test_data_agent = app.interactive_agent._get_data_agent()
                    if test_data_agent:
                        logger.info("✅ Data agent reference test successful")
                    else:
                        logger.warning("⚠️ Data agent reference test failed")
                        
            except Exception as e:
                logger.error(f"❌ Agent reference setup failed: {e}")
        
        # Call the function
        ensure_agent_references()


        # PHASE 11: Comprehensive diagnostic logging
        logger.info("🔬 Phase 11: Post-storage validation...")
        logger.info(f"🔬 DIAGNOSTIC: Current app instance: {app}")
        logger.info(f"🔬 DIAGNOSTIC: Current app id: {id(app)}")
        logger.info(f"🔬 DIAGNOSTIC: hasattr(app, 'interactive_agent'): {hasattr(app, 'interactive_agent')}")
        logger.info(f"🔬 DIAGNOSTIC: app.interactive_agent: {getattr(app, 'interactive_agent', 'NOT_FOUND')}")
        logger.info(f"🔬 DIAGNOSTIC: app.interactive_agent type: {type(getattr(app, 'interactive_agent', None))}")
        
        # Check app agent-related attributes
        agent_related = {k: v for k, v in app.__dict__.items() if 'agent' in k.lower()}
        logger.info(f"🔬 DIAGNOSTIC: App agent-related attributes: {list(agent_related.keys())}")
        
        logger.info(f"🔍 DEBUG: Stored in app.interactive_agent: {hasattr(app, 'interactive_agent')}")
        logger.info(f"🔍 DEBUG: app.interactive_agent value: {getattr(app, 'interactive_agent', 'NOT_FOUND')}")

        # PHASE 12: Setup agent broadcast references
        logger.info("📡 Phase 12: Setting up agent broadcast references...")
        try:
            if hasattr(app, 'data_agent'):
                app.data_agent._app = app
            if hasattr(app, 'market_tools'): 
                app.market_tools._app = app
            if hasattr(app, 'analysis_agent'):
                app.analysis_agent._app = app
            logger.info("✅ Agent broadcast references setup")
        except Exception as e:
            logger.warning(f"⚠️ Agent broadcast setup warning: {e}")

        
        # PHASE 14: Initialize Visual Agent
        logger.info("🎨 Phase 14: Initializing Visual Agent...")
        from visual_agent import create_visual_agent

        visual_agent = create_visual_agent(
            data_layer=data_layer,
            orchestrator=orchestrator,
            ai_config={
                'deepseek_api_key': os.getenv('DEEPSEEK_API_KEY'),
                'claude_api_key': os.getenv('ANTHROPIC_API_KEY'),
                'vision_enabled': False  # Optional feature, disabled by default
            }
        )
        logger.info("✅ Visual Agent created (self-registered)")



        # PHASE 13: Final verification
        logger.info("✅ Phase 13: Final verification...")
        if hasattr(orchestrator, 'agents'):
            agent_list = list(orchestrator.agents.keys())
            logger.info(f"📋 Registered Agents ({len(agent_list)}): {agent_list}")
            
            if len(agent_list) >= 4:
                logger.info(f"✅ Agent system initialized successfully! ({len(agent_list)} agents total)")
                _agent_system_initialized = True
                
                # Final success log
                logger.info("🎉 INITIALIZATION COMPLETE:")
                logger.info(f"   📊 Data Layer: Ready")
                logger.info(f"   📈 Market Tools: Ready") 
                logger.info(f"   🤖 Analysis Agent: Ready")
                logger.info(f"   🎯 Orchestrator: Ready with {len(agent_list)} agents")
                logger.info(f"   💬 Interactive Agent: Ready")
                logger.info(f"   📱 Flask App Integration: Complete")
                
                return True
            else:
                logger.error(f"❌ Insufficient agents registered ({len(agent_list)})")
                return False
        else:
            logger.error("❌ No agents registry found in orchestrator")
            return False
        
    except Exception as e:
        logger.error(f"❌ Agent initialization failed: {e}")
        logger.error(f"❌ Error type: {type(e).__name__}")
        import traceback
        logger.error(f"❌ Full traceback:\n{traceback.format_exc()}")

        # Store in Flask app
        app.visual_agent = visual_agent

        # Update Phase 13 verification count (change from >= 4 to >= 5)
        # Modify existing Phase 13 check:
        if len(agent_list) >= 5:  # Changed from 4 to 5
            logger.info(f"✅ Agent system initialized successfully ({len(agent_list)} agents total)")


        # Additional debugging information
        logger.error("🔍 Additional Debug Info:")
        logger.error(f"   Current working directory: {os.getcwd()}")
        logger.error(f"   Python path: {sys.path[:3]}...")  # First 3 entries
        logger.error(f"   Environment variables: DB_HOST={os.getenv('DB_HOST')}")
        
        return False

# =============================================================================
# SUMMARY OF CHANGES
# =============================================================================



#initialize_agents_for_flask_app()


# ============================================================================
# NEW: SERVICE MANAGEMENT FUNCTIONS (ADD TO app2.py)
# ============================================================================

# Global variable to track shutdown state
shutdown_flag = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully - COMPLETELY NEW FUNCTION"""
    global shutdown_flag
    signal_names = {signal.SIGTERM: 'SIGTERM', signal.SIGINT: 'SIGINT'}
    signal_name = signal_names.get(signum, f'Signal {signum}')

    logger.info(f"🛑 Received {signal_name}, initiating graceful shutdown...")
    shutdown_flag = True

    # Allow current requests to complete
    time.sleep(2)

    # Clean up connection pool
    cleanup_connection_pool()

    logger.info("✅ Graceful shutdown complete")
    sys.exit(0)

def cleanup_on_exit():
    """Cleanup function called on normal exit - COMPLETELY NEW FUNCTION"""
    logger.info("🧹 Cleaning up resources...")
    cleanup_connection_pool()  # Close all connections in the pool
    logger.info("✅ Cleanup complete")

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown - COMPLETELY NEW FUNCTION"""
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    atexit.register(cleanup_on_exit)
    logger.info("🔧 Signal handlers configured")

# FIXED: Safe port check that doesn't kill own process
def smart_port_check(port):
    """Smart port check that safely handles port conflicts without self-termination"""
    import socket
    import subprocess
    import time

    try:
        # Check if port is in use
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            result = s.connect_ex(('localhost', port))

            if result == 0:
                # Port is in use - check if it's our process
                logger.warning(f"⚠️  Port {port} already in use, checking process...")
                current_pid = os.getpid()

                try:
                    cmd = f"lsof -ti tcp:{port}"
                    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=3)
                    if result.stdout.strip():
                        pids = result.stdout.strip().split('\n')
                        our_process = False

                        for pid_str in pids:
                            try:
                                pid = int(pid_str.strip())
                                # CRITICAL: Never kill our own process
                                if pid == current_pid:
                                    our_process = True
                                    logger.info(f"✅ Port {port} is held by our process (PID {pid})")
                                    return True

                                # Also check if it's a parent/child process
                                if pid == os.getppid():
                                    logger.warning(f"⚠️  Port held by parent process (PID {pid}), skipping termination")
                                    # Wait and retry - don't kill parent
                                    time.sleep(1)
                                    continue

                            except (ValueError, IndexError):
                                continue

                        # If we get here and no process is ours, just log and fail gracefully
                        logger.warning(f"⚠️  Port {port} held by other process(es): {pids}")
                        logger.warning(f"⚠️  Skipping aggressive cleanup to prevent service conflicts")
                        # Return False so caller can handle appropriately
                        return False

                except subprocess.TimeoutExpired:
                    logger.warning(f"⚠️  Timeout checking port {port}, assuming in use")
                    return False
                except Exception as e:
                    logger.warning(f"⚠️ Port check error: {e}")
                    # If we can't determine, assume port is available (safer than killing)
                    return True
            else:
                logger.info(f"✅ Port {port} is available")
                return True

    except Exception as e:
        logger.warning(f"⚠️ Port check error: {e}")
        # Assume available if can't check (safer than failing)
        return True


# ============================================================================
# END OF NEW FUNCTIONS
# ============================================================================

# Add this after the agent initialization section
def setup_agent_broadcast_reference():
    """Store app reference for agent broadcasting"""
    if hasattr(app, 'interactive_agent'):
        # Pass app reference to agents
        if hasattr(app, 'data_agent'):
            app.data_agent._app = app
        if hasattr(app, 'market_tools'): 
            app.market_tools._app = app
        if hasattr(app, 'analysis_agent'):
            app.analysis_agent._app = app
        logger.info("✅ Agent broadcast references setup")

# Call it after agent creation
setup_agent_broadcast_reference()

# ============================================================================
# SIMPLE DATABASE CONNECTION
# ============================================================================

def get_db_connection():
    """Get a direct database connection. Caller must call conn.close() when done."""
    try:
        import psycopg2 as _pg2
        conn = _pg2.connect(
            host=app.config['DB_HOST'],
            database=app.config['DB_NAME'],
            user=app.config['DB_USER'],
            password=app.config['DB_PASSWORD'],
            connect_timeout=10,
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

def return_db_connection(conn):
    """Close a connection obtained via get_db_connection()."""
    if conn:
        try:
            conn.close()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")

def cleanup_connection_pool():
    """Close all connections in the pool"""
    global _connection_pool
    if _connection_pool:
        try:
            _connection_pool.closeall()
            logger.info("✅ Connection pool closed successfully")
        except Exception as e:
            logger.error(f"Error closing connection pool: {e}")

# ============================================================================
# SIMPLE AUTHENTICATION
# ============================================================================

def token_required(f):
    """Simple JWT token validation"""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization', '').replace('Bearer ', '')
        
        if not token:
            return jsonify({'error': 'Token missing'}), 401
            
        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
            
            conn = get_db_connection()
            if not conn:
                return jsonify({'error': 'Database connection failed'}), 500
                
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, username, role FROM users WHERE id = %s", (data['id'],))
                user = cursor.fetchone()
                
                if not user:
                    return jsonify({'error': 'User not found'}), 401
                    
                current_user = {'id': user[0], 'username': user[1], 'role': user[2]}
            
            conn.close()
                    
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token expired'}), 401
        except Exception as e:
            logger.error(f"Token validation error: {e}")
            return jsonify({'error': 'Invalid token'}), 401
            
        return f(current_user, *args, **kwargs)
    return decorated

# Connection pool is initialized per-worker via gunicorn post_worker_init hook.
# With preload_app=True, master-process connections would be invalid post-fork.

@app.route('/api/login', methods=['POST'])
def login():
    """User authentication"""
    try:
        data = request.json
        username = data.get('username')
        password = data.get('password')
        
        if not username or not password:
            return jsonify({'error': 'Username and password required'}), 400
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, username, password, role FROM users WHERE username = %s", (username,))
            user = cursor.fetchone()
            
            if not user or not check_password_hash(user[2], password):
                return jsonify({'error': 'Invalid credentials'}), 401
            
            token = jwt.encode({
                'id': user[0],
                'username': user[1],
                'exp': datetime.utcnow() + timedelta(hours=24)
            }, app.config['SECRET_KEY'], algorithm="HS256")
            
        conn.close()
        
        return jsonify({
            'token': token,
            'user': {'id': user[0], 'username': user[1], 'role': user[3]}
        })
                
    except Exception as e:
        logger.error(f"Login error: {e}")
        return jsonify({'error': 'Authentication failed'}), 500

# ============================================================================
# GLOBAL AGENT INSTANCES (Singleton Pattern)
# ============================================================================

# Global variables for agent instances
_interactive_agent = None
_orchestrator = None
_market_tools = None
_agent_initialization_lock = asyncio.Lock() if AGENTS_AVAILABLE else None





# ============================================================================
# KITE CONNECT HELPERS - SIMPLIFIED
# ============================================================================

class KiteHelper:
    """Simple Kite helper class"""
    
    @staticmethod
    def get_stored_token():
        """Get stored Kite token from unified api_tokens table"""
        try:
            conn = get_db_connection()
            if not conn:
                return None
            cursor = conn.cursor()
            cursor.execute("""
                SELECT token FROM api_tokens 
                WHERE broker_type = 'kite' 
                ORDER BY created_at DESC 
                LIMIT 1
            """)
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Failed to retrieve Kite access token: {e}")
            return None
    
    @staticmethod  
    def get_kite_client():
        """Get authenticated Kite client"""
        try:
            from kiteconnect import KiteConnect
            
            access_token = KiteHelper.get_stored_token()
            if not access_token:
                return None
                
            kite = KiteConnect(api_key=app.config['KITE_API_KEY'])
            kite.set_access_token(access_token)
            return kite
        except ImportError:
            logger.error("KiteConnect not installed. Install with: pip install kiteconnect")
            return None
        except Exception as e:
            logger.error(f"Failed to initialize Kite client: {e}")
            return None


# entry_price/entry_qty in trades_v2 are frozen at fill time and never adjusted
# for a later corporate action (split/bonus) on the underlying symbol. A >20%
# overnight jump in ltp/entry ratio is far more likely a stale pre-action entry
# than a genuine move — confirmed 2026-07-31: NARMADA 2:1 split, ratio 2.0066,
# produced a ~-4,263 fabricated unrealized P&L off an actual ~-14 Kite position
# (Kite adjusts held qty/avg price for the split; we never do). Suppress rather
# than display a number computed off mismatched pre/post-action price scales.
CORP_ACTION_RATIO_GUARD = 0.20

def _position_direction_qty_unrealized(p: dict, ltp_map: dict):
    """Direction/qty/current-price/unrealized P&L for one open master position.

    Returns (direction, qty, ltp, unrealized_rs) — unrealized_rs is None when
    no live LTP was available, or when the LTP/entry_price ratio is outside
    the corp-action guard band (position becomes un-priceable until entry_price/
    entry_qty are reconciled against the actual post-action Kite holding).
    """
    zone      = p.get('zone', '') or ''
    direction = 'SHORT' if 'SHORT' in zone.upper() else 'LONG'
    entry     = float(p.get('entry_price') or 0)
    capital   = float(p.get('capital_deployed') or 0)
    qty       = int(capital / entry) if entry > 0 else 0
    raw_ltp   = ltp_map.get(p.get('symbol'))
    ltp       = raw_ltp or entry

    corp_action_suspected = False
    if raw_ltp and entry > 0:
        ratio = ltp / entry
        corp_action_suspected = not (1 - CORP_ACTION_RATIO_GUARD <= ratio <= 1 + CORP_ACTION_RATIO_GUARD)

    if not raw_ltp or corp_action_suspected:
        unrl = None
    else:
        unrl = round((entry - ltp) * qty if direction == 'SHORT' else (ltp - entry) * qty, 2)
    return direction, qty, ltp, unrl

# ============================================================================
# BREEZE CONNECT HELPERS - SIMPLIFIED WITH ENCODING FIX
# ============================================================================

class BreezeHelper:
    """Simple Breeze helper class with fixed URL encoding"""
    
    BASE_URL = "https://api.icicidirect.com/breezeapi/api/v1"
    LOGIN_URL = "https://api.icicidirect.com/apiuser/login"
    
    @staticmethod
    def get_stored_session():
        """Get stored Breeze session token from unified api_tokens table"""
        try:
            conn = get_db_connection()
            if not conn:
                return None, None
            cursor = conn.cursor()
            cursor.execute("""
                SELECT token, token FROM api_tokens 
                WHERE broker_type = 'breeze' 
                AND (expires_at IS NULL OR expires_at > NOW()) 
                ORDER BY created_at DESC 
                LIMIT 1
            """)
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            # Return (api_session, session_token) - for Breeze they're the same
            return result if result else (None, None)
        except Exception as e:
            logger.error(f"Failed to retrieve Breeze session: {e}")
            return (None, None)
    
    @staticmethod
    def create_checksum(timestamp, payload, secret_key):
        """Create SHA256 checksum for Breeze API authentication"""
        checksum_string = timestamp + payload + secret_key
        return hashlib.sha256(checksum_string.encode('utf-8')).hexdigest()
    
    @staticmethod
    def get_timestamp():
        """Get ISO8601 UTC timestamp with 0 milliseconds"""
        return datetime.utcnow().isoformat()[:19] + '.000Z'
    
    @staticmethod
    def get_breeze_client():
        """Get authenticated Breeze client instance"""
        try:
            api_session, session_token = BreezeHelper.get_stored_session()
            if not api_session or not session_token:
                return None
                
            return BreezeClient(
                api_key=app.config['BREEZE_API_KEY'],
                secret_key=app.config['BREEZE_API_SECRET'],
                session_token=session_token
            )
        except Exception as e:
            logger.error(f"Failed to get Breeze client: {e}")
            return None

class BreezeClient:
    """Simple Breeze API client with HTTP requests"""
    
    def __init__(self, api_key, secret_key, session_token):
        self.api_key = api_key
        self.secret_key = secret_key
        self.session_token = session_token
        self.base_url = "https://api.icicidirect.com/breezeapi/api/v1"
    
    def _make_request(self, endpoint, method='GET', payload=None):
        """Make authenticated request to Breeze API"""
        try:
            url = f"{self.base_url}/{endpoint}"
            timestamp = BreezeHelper.get_timestamp()
            
            if payload is None:
                payload = {}
            json_payload = json.dumps(payload, separators=(',', ':'))
            
            checksum = BreezeHelper.create_checksum(timestamp, json_payload, self.secret_key)
            
            headers = {
                'Content-Type': 'application/json',
                'X-Checksum': f'token {checksum}',
                'X-Timestamp': timestamp,
                'X-AppKey': self.api_key,
                'X-SessionToken': self.session_token
            }
            
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                data=json_payload,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Breeze API error: {response.status_code} - {response.text}")
                return {'Status': response.status_code, 'Error': response.text}
                    
        except Exception as e:
            logger.error(f"Breeze API request failed: {e}")
            return {'Status': 500, 'Error': str(e)}
    
    # Portfolio Methods
    def get_portfolio_holdings(self):
        """Get portfolio holdings"""
        response = self._make_request('dematholdings')
        return response.get('Success', []) if response else []
    
    def get_portfolio_positions(self):
        """Get current portfolio positions"""
        response = self._make_request('portfoliopositions')
        return response.get('Success', []) if response else []
    
    def get_funds(self):
        """Get available funds"""
        response = self._make_request('funds')
        return response.get('Success', {}) if response else {}
    
    def profile(self):
        """Get user profile - SIMPLE NULL CHECK"""
        try:
            payload = {
                'SessionToken': self.session_token,
                'AppKey': self.api_key
            }
            
            url = "https://api.icicidirect.com/breezeapi/api/v1/customerdetails"
            headers = {'Content-Type': 'application/json'}
            
            response = requests.get(url, headers=headers, data=json.dumps(payload), timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                # THE ACTUAL FIX: Handle None directly
                if data is None:
                    logger.error("API returned None")
                    return {'user_id': 'breeze_user', 'user_name': 'Breeze User', 'email': 'breeze_user', 'last_login': 'Recently'}
                
                success_data = data.get('Success', {})
                return {
                    'user_id': success_data.get('idirect_userid') or 'breeze_user',
                    'user_name': success_data.get('idirect_user_name') or 'Breeze User',
                    'email': success_data.get('idirect_userid') or 'breeze_user',
                    'last_login': success_data.get('idirect_lastlogin_time') or 'Recently'
                }
            
            return {'user_id': 'breeze_user', 'user_name': 'Breeze User', 'email': 'breeze_user', 'last_login': 'Recently'}
        except Exception as e:
            logger.error(f"Profile fetch error: {e}")
            return {'user_id': 'breeze_user', 'user_name': 'Breeze User', 'email': 'breeze_user', 'last_login': 'Recently'}

class ALADINDataAccess:
    """
    Database access layer for ALADIN web interface
    Provides optimized queries for chart data, news events, and indicators
    """
    
    def __init__(self, connection_pool_size=5):
        """Initialize with connection management for concurrent users"""
        self.pool_size = connection_pool_size
        
    def get_connection(self):
        """Get database connection using existing app pattern"""
        return get_db_connection()
    
    def get_chart_data(self, symbol: str, days: int = 7) -> dict:
        """
        Get OHLCV data with technical indicators for chart visualization
        
        Args:
            symbol: Stock symbol (e.g., 'VOLTAMP')
            days: Number of days of historical data
            
        Returns:
            {
                'success': True,
                'symbol': 'VOLTAMP',
                'data': [
                    {
                        'timestamp': '2025-08-03T09:15:00Z',
                        'open': 1250.0,
                        'high': 1275.0,
                        'low': 1240.0,
                        'close': 1265.0,
                        'volume': 125000,
                        'indicators': {...}
                    }
                ]
            }
        """
        conn = self.get_connection()
        if not conn:
            return {'success': False, 'error': 'Database connection failed'}
            
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        timestamp,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        indicators
                    FROM market_data_enhanced 
                    WHERE symbol = %s 
                        AND timestamp >= NOW() - INTERVAL '%s days'
                    ORDER BY timestamp ASC
                """, (symbol.upper(), days))
                
                rows = cursor.fetchall()
                
                chart_data = []
                for row in rows:
                    chart_data.append({
                        'timestamp': row[0].isoformat() if row[0] else None,
                        'open': float(row[1]) if row[1] else 0,
                        'high': float(row[2]) if row[2] else 0,
                        'low': float(row[3]) if row[3] else 0,
                        'close': float(row[4]) if row[4] else 0,
                        'volume': int(row[5]) if row[5] else 0,
                        'indicators': row[6] if row[6] else {}
                    })
                
                return {
                    'success': True,
                    'symbol': symbol.upper(),
                    'data': chart_data,
                    'count': len(chart_data)
                }
                
        except Exception as e:
            logger.error(f"Chart data error for {symbol}: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            if conn:
                conn.close()
    
    def get_news_timeline(self, symbol: str, days: int = 7) -> dict:
        """
        Get news events for timeline markers on charts
        
        Returns:
            {
                'success': True,
                'symbol': 'VOLTAMP',
                'events': [
                    {
                        'id': 123,
                        'headline': 'Q3 Results Announced',
                        'summary': 'Strong quarterly results...',
                        'sentiment_score': 7.5,
                        'impact_tier': 'high',
                        'published_at': '2025-08-01T10:30:00Z'
                    }
                ]
            }
        """
        conn = self.get_connection()
        if not conn:
            return {'success': False, 'error': 'Database connection failed'}
            
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        ni.id,
                        ni.headline,
                        ni.summary,
                        ni.sentiment_score,
                        ni.news_tier,
                        ni.published_at,
                        ni.standardized_event_type
                    FROM news_insights ni
                    WHERE %s = ANY(
                        SELECT jsonb_array_elements_text(ni.affected_symbols)
                    )
                        AND ni.published_at >= NOW() - INTERVAL '%s days'
                        AND ni.sentiment_score IS NOT NULL
                        AND ni.news_tier IS NOT NULL
                    ORDER BY ni.published_at DESC
                """, (symbol.upper(), days))
                
                rows = cursor.fetchall()
                
                news_events = []
                for row in rows:
                    news_events.append({
                        'id': row[0],
                        'headline': row[1],
                        'summary': row[2],
                        'sentiment_score': float(row[3]) if row[3] else 5.0,
                        'impact_tier': row[4],
                        'published_at': row[5].isoformat() if row[5] else None,
                        'event_type': row[6]
                    })
                
                return {
                    'success': True,
                    'symbol': symbol.upper(),
                    'events': news_events,
                    'count': len(news_events)
                }
                
        except Exception as e:
            logger.error(f"News timeline error for {symbol}: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            if conn:
                conn.close()

    def get_indicators_data(self, symbol: str, days: int = 7) -> dict:
        """
        Get technical indicators data for symbol
        
        Returns formatted indicators from JSONB column
        """
        conn = self.get_connection()
        if not conn:
            return {'success': False, 'error': 'Database connection failed'}
            
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        timestamp,
                        close,
                        indicators
                    FROM market_data_enhanced 
                    WHERE symbol = %s 
                        AND timestamp >= NOW() - INTERVAL '%s days'
                        AND indicators IS NOT NULL
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (symbol.upper(), days))
                
                row = cursor.fetchone()
                
                if row:
                    return {
                        'success': True,
                        'symbol': symbol.upper(),
                        'timestamp': row[0].isoformat() if row[0] else None,
                        'current_price': float(row[1]) if row[1] else 0,
                        'indicators': row[2] if row[2] else {}
                    }
                else:
                    return {
                        'success': True,
                        'symbol': symbol.upper(),
                        'indicators': {},
                        'message': 'No recent indicator data available'
                    }
                
        except Exception as e:
            logger.error(f"Indicators data error for {symbol}: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            if conn:
                conn.close()


# ============================================================================
# DATABASE INITIALIZATION - SIMPLIFIED
# ============================================================================

def initialize_database():
    """Initialize essential database tables"""
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Cannot initialize database - connection failed")
            return False
            
        with conn.cursor() as cursor:
            # Create users table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password VARCHAR(100) NOT NULL,
                    role VARCHAR(20) NOT NULL DEFAULT 'user',
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Create API tokens table (unified for Kite and Breeze)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS api_tokens (
                    id SERIAL PRIMARY KEY,
                    token TEXT NOT NULL,
                    broker_type VARCHAR(20) DEFAULT 'kite',
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMP DEFAULT NULL
                )
            """)
            
            # Add default admin user if none exists
            cursor.execute("SELECT COUNT(*) FROM users")
            if cursor.fetchone()[0] == 0:
                password_hash = generate_password_hash('Covig@2025')
                cursor.execute(
                    "INSERT INTO users (username, password, role) VALUES (%s, %s, %s)",
                    ('BackendRESTAPIStockmart', password_hash, 'admin')
                )
                logger.info("Created default admin user")
            
            conn.commit()
            logger.info("✅ Database initialization completed")
            
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return False

# ============================================================================
# CORE API ENDPOINTS
# ============================================================================

@app.route('/api/strategies/live', methods=['GET'])
def strategies_live():
    """Return live/shadow status for all strategies from tradingv2 config.
    Used by the buddies page to automatically reflect entry_enabled toggles."""
    try:
        import importlib, sys as _sys
        tradingv2_path = '/opt/stockapp'
        if tradingv2_path not in _sys.path:
            _sys.path.insert(0, tradingv2_path)
        config = importlib.import_module('tradingv2.config')
        importlib.reload(config)  # always read current state from disk

        result = {}
        for sid, cfg in config.STRATEGY_CONFIG.items():
            result[sid] = {
                'live': bool(cfg.get('enabled', False) and cfg.get('entry_enabled', False)),
                'description': cfg.get('description', ''),
            }
        return jsonify(result)
    except Exception as e:
        logger.error(f"strategies_live error: {e}")
        return jsonify({}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Simple system health check"""
    try:
        # Check database
        conn = get_db_connection()
        if conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                db_status = "connected"
            return_db_connection(conn)
        else:
            db_status = "disconnected"
        
        return jsonify({
            'status': 'healthy' if db_status == 'connected' else 'degraded',
            'database': db_status,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# ============================================================================
# KITE CONNECT AUTHENTICATION - PRESERVED & SIMPLIFIED
# ============================================================================

@app.route('/api/auth/kite/login', methods=['GET'])
def kite_auth_login():
    """Initiate Kite Connect authentication"""
    try:
        if not app.config['KITE_API_KEY']:
            return jsonify({'error': 'Kite API key not configured'}), 500
        
        kite_login_url = f"https://kite.zerodha.com/connect/login?api_key={app.config['KITE_API_KEY']}&v=3&state=owner"
        
        return jsonify({
            'auth_url': kite_login_url,
            'redirect_url': app.config['KITE_REDIRECT_URL']
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/kite/callback', methods=['GET'])
def kite_callback_fixed():
    """Handle Kite Connect callback and store token"""
    try:
        request_token = request.args.get('request_token')
        if not request_token:
            return "Missing request token", 400

        # ── Owner login (state=owner) ──
        if request.args.get('state') == 'owner':
            logger.info("[kite_callback] → routing to owner flow")
            return _kite_owner_callback()

        # ── Subscriber sign-in: state=browse is set by /api/subscribe/signin ──
        if request.args.get('state') == 'browse':
            logger.info("[kite_callback] state=browse → routing to subscribe flow")
            return kite_subscribe_callback()

        # ── Fallback: IP-based detection (for older flows without state param) ──
        client_ip = request.headers.get('X-Real-IP') or request.remote_addr
        _is_subscribe = _sub_check_and_consume_ip(client_ip) or request.args.get('strategy_id')
        logger.info(f"[kite_callback] ip={client_ip} is_subscribe={bool(_is_subscribe)}")
        if _is_subscribe:
            logger.info("[kite_callback] → routing to subscribe flow (IP match)")
            return kite_subscribe_callback()

        logger.info(f"[kite_callback] no state param + not subscribe → routing to owner flow")
        return _kite_owner_callback()

    except Exception as e:
        logger.error(f"❌ Kite authentication failed: {str(e)}")
        return f"Authentication failed: {str(e)}", 500


@app.route('/api/auth/kite/status', methods=['GET'])
@token_required
def kite_auth_status(current_user):
    """Check Kite authentication status"""
    try:
        access_token = KiteHelper.get_stored_token()
        
        if not access_token:
            return jsonify({
                'authenticated': False,
                'message': 'No Kite access token found'
            })
        
        kite = KiteHelper.get_kite_client()
        if not kite:
            return jsonify({
                'authenticated': False,
                'error': 'Could not initialize Kite client'
            })
        
        try:
            profile = kite.profile()
            return jsonify({
                'authenticated': True,
                'user_id': profile.get('user_id'),
                'user_name': profile.get('user_name'),
                'email': profile.get('email')
            })
            
        except Exception as e:
            return jsonify({
                'authenticated': False,
                'error': 'Token validation failed',
                'message': str(e)
            })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/auth/kite/refresh', methods=['POST'])
@token_required
def kite_auth_refresh(current_user):
    """Refresh Kite authentication"""
    return jsonify({
        'message': 'Kite tokens require manual refresh',
        'auth_url': "/api/auth/kite/login"
    })

# ============================================================================
# BREEZE CONNECT AUTHENTICATION - FIXED ENCODING
# ============================================================================

@app.route('/api/auth/breeze/login', methods=['GET'])
def breeze_auth_login():
    """Initiate Breeze authentication - FIXED URL encoding"""
    try:
        breeze_api_key = app.config.get('BREEZE_API_KEY')
        
        if not breeze_api_key:
            return jsonify({
                'error': 'Breeze API key not configured',
                'instructions': 'Add BREEZE_API_KEY to your .env file'
            }), 500
        
        # CRITICAL FIX: Proper URL encoding for API key
        encoded_api_key = urllib.parse.quote(breeze_api_key, safe='')
        auth_url = f"https://api.icicidirect.com/apiuser/login?api_key={encoded_api_key}"
        
        logger.info(f"Generated Breeze auth URL with encoded API key")
        
        return jsonify({
            'auth_url': auth_url,
            'redirect_url': app.config['BREEZE_REDIRECT_URL'],
            'instructions': [
                'Click the link to open ICICI Direct login',
                'Login with your ICICI Direct credentials', 
                'After successful login, you will be redirected back'
            ]
        })
        
    except Exception as e:
        logger.error(f"Breeze auth URL generation error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/breeze/callback', methods=['GET', 'POST'])
def breeze_callback():
    """Handle Breeze OAuth callback - FIXED to handle both GET and POST"""
    try:
        # Handle both GET and POST requests
        if request.method == 'GET':
            api_session = request.args.get('api_session') or request.args.get('apisession')
            error_code = request.args.get('error')
            error_message = request.args.get('error_description')
        else:  # POST
            # Handle POST request with form data or JSON
            if request.is_json:
                data = request.json
                api_session = data.get('api_session') or data.get('apisession')
                error_code = data.get('error')
                error_message = data.get('error_description')
            else:
                # Form data or query params in POST
                api_session = request.form.get('api_session') or request.form.get('apisession') or request.args.get('apisession')
                error_code = request.form.get('error') or request.args.get('error')
                error_message = request.form.get('error_description') or request.args.get('error_description')
        
        logger.info(f"Breeze callback: method={request.method}, api_session={api_session[:10] if api_session else None}...")

        # ── Subscriber sign-in: IP registered by /api/subscribe/breeze-signin ──
        client_ip = request.headers.get('X-Real-IP') or request.remote_addr
        if _sub_breeze_check_and_consume_ip(client_ip):
            logger.info(f"[breeze_callback] ip={client_ip} → routing to subscribe flow")
            return breeze_subscribe_callback()

        # Check for error conditions first
        if error_code:
            logger.error(f"Breeze callback error: {error_code} - {error_message}")
            return f"""
            <h1>❌ Breeze Authentication Failed</h1>
            <p><strong>Error:</strong> {error_code}</p>
            <p><strong>Message:</strong> {error_message or 'Unknown error'}</p>
            <script>setTimeout(() => window.close(), 5000);</script>
            """, 400
        
        if not api_session:
            logger.error("Breeze callback missing api_session parameter")
            return """
            <h1>❌ Authentication Error</h1>
            <p>Missing API session parameter</p>
            <script>setTimeout(() => window.close(), 3000);</script>
            """, 400
        
        logger.info(f"Processing Breeze callback with session: {api_session[:10]}...")

        # The api_session from Zerodha OAuth callback is already valid
        # We can save it directly without additional validation
        session_token = api_session

        logger.info(f"Breeze session token received: {session_token[:15]}...")

        # Store session in unified api_tokens table
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cursor:
                    # Clean old Breeze sessions first
                    cursor.execute("DELETE FROM api_tokens WHERE broker_type = 'breeze' AND expires_at < NOW()")
                    deleted = cursor.rowcount
                    logger.info(f"Cleaned up {deleted} expired Breeze tokens")

                    # Insert new Breeze session - the api_session itself is the valid token
                    cursor.execute("""
                        INSERT INTO api_tokens (token, broker_type, created_at, expires_at)
                        VALUES (%s, 'breeze', NOW(), NOW() + INTERVAL '24 hours')
                    """, (session_token,))
                    logger.info(f"Inserted new Breeze token: {session_token[:15]}...")

                conn.commit()
                conn.close()

                logger.info(f"✅ Breeze authentication successful - Saved token: {session_token[:15] if session_token else 'None'}...")

                return f"""
                <h1>✅ Breeze Authentication Successful!</h1>
                <p><strong>Session Token:</strong> {session_token[:20]}...</p>
                <p><strong>Validity:</strong> 24 hours</p>
                <p>You can now close this window and return to the application.</p>
                <script>setTimeout(() => window.close(), 3000);</script>
                """
            except Exception as db_error:
                logger.error(f"❌ Failed to save Breeze token to database: {db_error}")
                import traceback
                logger.error(traceback.format_exc())
                if conn:
                    conn.close()
                return f"""
                <h1>❌ Database Error</h1>
                <p>Could not store authentication session</p>
                <p><small>Error: {str(db_error)[:100]}</small></p>
                <script>setTimeout(() => window.close(), 3000);</script>
                """, 500
        else:
            logger.error("❌ Could not get database connection in breeze_callback")
            return """
            <h1>❌ Database Error</h1>
            <p>Could not get database connection</p>
            <script>setTimeout(() => window.close(), 3000);</script>
            """, 500
            
    except Exception as e:
        logger.error(f"Breeze callback error: {e}")
        return f"""
        <h1>❌ System Error</h1>
        <p>Authentication callback failed</p>
        <script>setTimeout(() => window.close(), 5000);</script>
        """, 500

@app.route('/api/auth/breeze/status', methods=['GET'])
@token_required
def breeze_auth_status(current_user):
    """Check Breeze authentication status"""
    try:
        breeze_api_key = app.config.get('BREEZE_API_KEY')
        breeze_api_secret = app.config.get('BREEZE_API_SECRET')
        
        # Check basic configuration
        if not breeze_api_key:
            return jsonify({
                'authenticated': False,
                'message': 'Breeze API key not configured',
                'configured': False
            })
        
        if not breeze_api_secret:
            return jsonify({
                'authenticated': False,
                'message': 'Breeze API secret not configured',
                'configured': False
            })
        
        # Check for active session
        api_session, session_token = BreezeHelper.get_stored_session()
        
        if not session_token:
            return jsonify({
                'authenticated': False,
                'message': 'No active Breeze session found',
                'configured': True
            })
        
        # Test session validity
        breeze_client = BreezeHelper.get_breeze_client()
        if breeze_client:
            try:
                profile = breeze_client.profile()
                if profile:
                    return jsonify({
                        'authenticated': True,
                        'user_id': profile.get('user_id'),
                        'user_name': profile.get('user_name'),
                        'configured': True
                    })
            except Exception as profile_error:
                logger.warning(f"Breeze profile fetch failed: {profile_error}")
        
        return jsonify({
            'authenticated': False,
            'message': 'Session expired or invalid',
            'configured': True
        })
        
    except Exception as e:
        logger.error(f"Breeze auth status error: {e}")
        return jsonify({
            'authenticated': False,
            'error': str(e),
            'configured': False
        }), 500

@app.route('/api/breeze/generate_session', methods=['POST'])
@token_required
def breeze_generate_session(current_user):
    """Generate Breeze session from API session"""
    try:
        data = request.json
        api_session = data.get('api_session')
        
        if not api_session:
            return jsonify({'error': 'API session required'}), 400
        
        # Get session token using customer details API
        customer_payload = {
            'SessionToken': api_session,
            'AppKey': app.config['BREEZE_API_KEY']
        }
        
        customer_response = requests.get(
            "https://api.icicidirect.com/breezeapi/api/v1/customerdetails",
            headers={'Content-Type': 'application/json'},
            data=json.dumps(customer_payload),
            timeout=30
        )
        
        if customer_response.status_code == 200:
            customer_data = customer_response.json()
            session_token = customer_data.get('Success', {}).get('session_token')
            user_id = customer_data.get('Success', {}).get('idirect_userid')
            
            if session_token:
                # Store in unified api_tokens table
                conn = get_db_connection()
                if conn:
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO api_tokens (token, broker_type, created_at, expires_at) 
                            VALUES (%s, 'breeze', NOW(), NOW() + INTERVAL '24 hours')
                        """, (session_token,))
                    conn.commit()
                    conn.close()
                    
                return jsonify({
                    'success': True,
                    'user_id': user_id,
                    'message': 'Session generated successfully'
                })
            else:
                return jsonify({'error': 'Failed to generate session token'}), 400
        else:
            return jsonify({'error': 'Customer details API failed'}), 400
        
    except Exception as e:
        logger.error(f"Breeze session generation error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/auth/breeze/refresh', methods=['POST'])
@token_required
def breeze_auth_refresh(current_user):
    """Refresh Breeze authentication"""
    return jsonify({
        'message': 'Breeze sessions require manual refresh',
        'auth_url': "/api/auth/breeze/login"
    })


# ============================================================================
# PORTFOLIO ENDPOINTS - SIMPLIFIED
# ============================================================================

@app.route('/api/portfolio/holdings', methods=['GET'])
@token_required
def get_portfolio_holdings(current_user):
    """Get current portfolio holdings from Kite API"""
    try:
        kite_client = KiteHelper.get_kite_client()
        if not kite_client:
            return jsonify({
                'success': False,
                'error': 'Kite client not available. Please authenticate first.'
            }), 401
        
        holdings = kite_client.holdings()
        
        return jsonify({
            'success': True,
            'holdings': holdings,
            'count': len(holdings),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Portfolio holdings error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/portfolio/positions', methods=['GET'])
@token_required
def get_portfolio_positions(current_user):
    """Get current portfolio positions from Kite API"""
    try:
        kite_client = KiteHelper.get_kite_client()
        if not kite_client:
            return jsonify({
                'success': False,
                'error': 'Kite client not available. Please authenticate first.'
            }), 401
        
        positions_response = kite_client.positions()
        
        return jsonify({
            'success': True,
            'positions': positions_response,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Portfolio positions error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/portfolio/holdings/breeze', methods=['GET'])
@token_required
def get_breeze_portfolio_holdings(current_user):
    """Get current portfolio holdings from Breeze API"""
    try:
        breeze_client = BreezeHelper.get_breeze_client()
        if not breeze_client:
            return jsonify({
                'success': False,
                'error': 'Breeze client not available. Please authenticate first.'
            }), 401
        
        holdings_response = breeze_client.get_portfolio_holdings()
        
        return jsonify({
            'success': True,
            'holdings': holdings_response,
            'source': 'breeze_api',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Breeze portfolio holdings error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/portfolio/positions/breeze', methods=['GET'])  
@token_required
def get_breeze_portfolio_positions(current_user):
    """Get current portfolio positions from Breeze API"""
    try:
        breeze_client = BreezeHelper.get_breeze_client()
        if not breeze_client:
            return jsonify({
                'success': False,
                'error': 'Breeze client not available. Please authenticate first.'
            }), 401
        
        positions_response = breeze_client.get_portfolio_positions()
        
        return jsonify({
            'success': True,
            'positions': positions_response,
            'source': 'breeze_api',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Breeze portfolio positions error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/portfolio/funds/breeze', methods=['GET'])
@token_required
def get_breeze_funds(current_user):
    """Get available funds from Breeze API"""
    try:
        breeze_client = BreezeHelper.get_breeze_client()
        if not breeze_client:
            return jsonify({
                'success': False,
                'error': 'Breeze client not available. Please authenticate first.'
            }), 401
        
        funds_response = breeze_client.get_funds()
        
        return jsonify({
            'success': True,
            'funds': funds_response,
            'source': 'breeze_api',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Breeze funds error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# ============================================================================
# COPY-TRADING SUBSCRIBER ENDPOINTS
# Each logged-in user connects their OWN broker account and selects strategies.
# Signals from the master tradingv2 system fan out to all active subscribers.
# ============================================================================

# Temporary browse sessions: {sid → {user_id, kite_user_id, dmat_name, expires}}
# Created when Kite OAuth completes in "browse" mode (state=browse).
# Consumed by /api/subscribe/confirm to create the subscription without a second OAuth.
import threading as _threading, json as _json, time as _sub_time
_browse_sessions: dict = {}
_browse_sessions_lock = _threading.Lock()

_SUBSCRIBE_PENDING_FILE = '/tmp/subscribe_pending_ips.json'
_BROWSE_SESSIONS_FILE   = '/tmp/subscribe_browse_sessions.json'

def _sub_register_ip(ip: str):
    """Write IP to shared file (works across gunicorn workers)."""
    try:
        try:
            data = _json.loads(open(_SUBSCRIBE_PENDING_FILE).read())
        except Exception:
            data = {}
        data[ip] = _sub_time.time() + 600
        open(_SUBSCRIBE_PENDING_FILE, 'w').write(_json.dumps(data))
    except Exception as e:
        logger.warning(f"[subscribe] _sub_register_ip failed: {e}")

def _sub_check_and_consume_ip(ip: str) -> bool:
    """Return True if IP was pending subscribe, and remove it."""
    try:
        data = _json.loads(open(_SUBSCRIBE_PENDING_FILE).read())
        ts = data.pop(ip, 0)
        open(_SUBSCRIBE_PENDING_FILE, 'w').write(_json.dumps(data))
        return ts > _sub_time.time()
    except Exception:
        return False

def _browse_session_write(sid: str, user_id, kite_user_id, dmat_name, broker_type='kite'):
    """Persist browse session to shared file (survives across gunicorn workers)."""
    try:
        try:
            data = _json.loads(open(_BROWSE_SESSIONS_FILE).read())
        except Exception:
            data = {}
        # Purge expired while we're here
        now = _sub_time.time()
        data = {k: v for k, v in data.items() if v.get('expires', 0) > now}
        data[sid] = {'user_id': user_id, 'kite_user_id': kite_user_id,
                     'dmat_name': dmat_name, 'broker_type': broker_type, 'expires': now + 900}
        open(_BROWSE_SESSIONS_FILE, 'w').write(_json.dumps(data))
    except Exception as e:
        logger.warning(f"[subscribe] _browse_session_write failed: {e}")

def _browse_session_consume(sid: str):
    """Return session dict and remove it; None if missing/expired."""
    try:
        data = _json.loads(open(_BROWSE_SESSIONS_FILE).read())
        sess = data.pop(sid, None)
        open(_BROWSE_SESSIONS_FILE, 'w').write(_json.dumps(data))
        if sess and sess.get('expires', 0) > _sub_time.time():
            return sess
    except Exception:
        pass
    return None


# One-time dashboard auth tokens: bypass Flask cookie issues after OAuth redirect.
# Generated by subscribe_confirm(), consumed by /api/subscribe/enter-dashboard.
_DASHBOARD_TOKENS_FILE = '/tmp/subscribe_dashboard_tokens.json'

def _dashboard_token_write(token: str, user_id, user_name: str, kite_id: str, role: str):
    try:
        try:
            data = _json.loads(open(_DASHBOARD_TOKENS_FILE).read())
        except Exception:
            data = {}
        now = _sub_time.time()
        data = {k: v for k, v in data.items() if v.get('expires', 0) > now}
        data[token] = {
            'user_id': user_id, 'user_name': user_name, 'kite_id': kite_id,
            'role': role, 'expires': now + 300,
        }
        open(_DASHBOARD_TOKENS_FILE, 'w').write(_json.dumps(data))
    except Exception as e:
        logger.warning(f"[dashboard_token] write failed: {e}")

def _dashboard_token_consume(token: str):
    try:
        data = _json.loads(open(_DASHBOARD_TOKENS_FILE).read())
        info = data.pop(token, None)
        open(_DASHBOARD_TOKENS_FILE, 'w').write(_json.dumps(data))
        if info and info.get('expires', 0) > _sub_time.time():
            return info
    except Exception:
        pass
    return None

def _auto_enter_dashboard_page(token: str) -> str:
    """A real (200) HTML response that immediately navigates to enter-dashboard.
    Setting the Flask session directly inside an OAuth-callback response and
    redirecting straight to /dashboard is unreliable — the Set-Cookie doesn't
    reliably survive being part of the same automatic redirect chain Kite/Breeze
    initiated (see _dashboard_token_write/enter-dashboard docstrings). Returning
    a genuine page here, whose JS then triggers a fresh first-party navigation,
    is the same bypass _kite_owner_callback already relies on."""
    dest = f'/api/subscribe/enter-dashboard?token={token}'
    return f'''<!doctype html><html><head><meta charset=utf-8><title>Logging in…</title>
<meta http-equiv="refresh" content="0;url={dest}">
</head><body><script>window.location.replace('{dest}');</script>
<p>Completing login…</p></body></html>'''

@app.route('/api/auth/kite/user-login', methods=['GET'])
@token_required
def kite_user_login(current_user):
    """Initiate Kite OAuth for the currently logged-in user."""
    if not app.config.get('KITE_API_KEY'):
        return jsonify({'error': 'Kite API key not configured'}), 500
    auth_url = (
        f"https://kite.zerodha.com/connect/login"
        f"?api_key={app.config['KITE_API_KEY']}&v=3"
    )
    callback = request.host_url.rstrip('/') + f'/api/kite/user-callback?user_id={current_user["id"]}'
    return jsonify({'auth_url': auth_url, 'callback_url': callback})


@app.route('/api/kite/user-callback', methods=['GET'])
def kite_user_callback():
    """
    Kite OAuth callback for a subscriber.
    user_id passed as query param (set in the redirect URL by kite_user_login).
    Stores token in user_broker_sessions instead of the shared api_tokens table.
    """
    try:
        request_token = request.args.get('request_token')
        user_id       = request.args.get('user_id')
        if not request_token or not user_id:
            return "Missing request_token or user_id", 400

        kite = KiteConnect(api_key=app.config['KITE_API_KEY'])
        session_data = kite.generate_session(
            request_token = request_token,
            api_secret    = app.config['KITE_API_SECRET'],
        )
        access_token = session_data['access_token']

        import psycopg2 as _pg
        conn = _pg.connect(
            host=app.config['DB_HOST'], database=app.config['DB_NAME'],
            user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_broker_sessions
                    (user_id, broker_type, api_key, session_token, expires_at, is_active)
                VALUES (%s, 'kite', %s, %s, NOW() + INTERVAL '1 day', TRUE)
                ON CONFLICT (user_id, broker_type) DO UPDATE SET
                    api_key       = EXCLUDED.api_key,
                    session_token = EXCLUDED.session_token,
                    expires_at    = EXCLUDED.expires_at,
                    is_active     = TRUE,
                    created_at    = NOW()
                """,
                (user_id, app.config['KITE_API_KEY'], access_token),
            )
        conn.commit()
        conn.close()
        logger.info(f"[copy-trading] Kite session saved for user_id={user_id}")
        return (
            "<h1>✅ Kite Connected!</h1>"
            "<p>Your Kite account is now linked. You can close this window and select strategies.</p>"
            "<script>setTimeout(() => window.close(), 3000);</script>"
        )
    except Exception as e:
        logger.error(f"[copy-trading] kite_user_callback error: {e}")
        return f"Authentication failed: {e}", 500


@app.route('/api/auth/breeze/user-login', methods=['GET'])
@token_required
def breeze_user_login(current_user):
    """Initiate Breeze OAuth for the currently logged-in user."""
    import urllib.parse
    api_key = app.config.get('BREEZE_API_KEY', '')
    if not api_key:
        return jsonify({'error': 'Breeze API key not configured'}), 500
    encoded_key = urllib.parse.quote(api_key, safe='')
    callback = request.host_url.rstrip('/') + f'/api/breeze/user-callback?user_id={current_user["id"]}'
    auth_url = f"https://api.icicidirect.com/apiuser/login?api_key={encoded_key}"
    return jsonify({'auth_url': auth_url, 'callback_url': callback})


@app.route('/api/breeze/user-callback', methods=['GET', 'POST'])
def breeze_user_callback():
    """
    Breeze OAuth callback for a subscriber.
    Receives api_session and stores it in user_broker_sessions.
    """
    try:
        if request.method == 'POST':
            data = request.get_json(silent=True) or request.form
        else:
            data = request.args
        api_session = data.get('api_session') or data.get('token')
        user_id     = data.get('user_id') or request.args.get('user_id')
        if not api_session or not user_id:
            return jsonify({'error': 'Missing api_session or user_id'}), 400

        import psycopg2 as _pg
        conn = _pg.connect(
            host=app.config['DB_HOST'], database=app.config['DB_NAME'],
            user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_broker_sessions
                    (user_id, broker_type, api_key, session_token, expires_at, is_active)
                VALUES (%s, 'breeze', %s, %s, NOW() + INTERVAL '1 day', TRUE)
                ON CONFLICT (user_id, broker_type) DO UPDATE SET
                    api_key       = EXCLUDED.api_key,
                    session_token = EXCLUDED.session_token,
                    expires_at    = EXCLUDED.expires_at,
                    is_active     = TRUE,
                    created_at    = NOW()
                """,
                (user_id, app.config.get('BREEZE_API_KEY', ''), api_session),
            )
        conn.commit()
        conn.close()
        logger.info(f"[copy-trading] Breeze session saved for user_id={user_id}")
        return (
            "<h1>✅ Breeze Connected!</h1>"
            "<p>Your ICICI Breeze account is now linked. You can close this window and select strategies.</p>"
            "<script>setTimeout(() => window.close(), 3000);</script>"
        )
    except Exception as e:
        logger.error(f"[copy-trading] breeze_user_callback error: {e}")
        return f"Authentication failed: {e}", 500


@app.route('/api/subscriptions', methods=['GET', 'POST', 'DELETE'])
@token_required
def user_subscriptions(current_user):
    """
    GET  — list this user's strategy subscriptions
    POST — subscribe to a strategy: {strategy_id, capital_per_trade}
    DELETE — unsubscribe: {strategy_id}
    """
    import psycopg2 as _pg
    user_id = current_user['id']

    def _conn():
        return _pg.connect(
            host=app.config['DB_HOST'], database=app.config['DB_NAME'],
            user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
            connect_timeout=5,
        )

    if request.method == 'GET':
        try:
            conn = _conn()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT uss.strategy_id, uss.capital_per_trade, uss.enabled, uss.created_at,
                           ubs.broker_type, ubs.is_active, ubs.expires_at
                    FROM user_strategy_subscriptions uss
                    LEFT JOIN user_broker_sessions ubs ON ubs.user_id = uss.user_id
                    WHERE uss.user_id = %s
                    ORDER BY uss.strategy_id
                    """,
                    (user_id,),
                )
                rows = cur.fetchall()
            conn.close()
            subs = [
                {
                    'strategy_id':       r[0],
                    'capital_per_trade': r[1],
                    'enabled':           r[2],
                    'created_at':        r[3].isoformat() if r[3] else None,
                    'broker_type':       r[4],
                    'broker_active':     r[5],
                    'broker_expires':    r[6].isoformat() if r[6] else None,
                }
                for r in rows
            ]
            return jsonify({'success': True, 'subscriptions': subs})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500

    elif request.method == 'POST':
        data = request.get_json(silent=True) or {}
        strategy_id       = data.get('strategy_id', '').strip().upper()
        capital_per_trade = int(data.get('capital_per_trade', 0))
        if not strategy_id or capital_per_trade <= 0:
            return jsonify({'error': 'strategy_id and capital_per_trade required'}), 400
        try:
            conn = _conn()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_strategy_subscriptions
                        (user_id, strategy_id, capital_per_trade, enabled)
                    VALUES (%s, %s, %s, TRUE)
                    ON CONFLICT (user_id, strategy_id) DO UPDATE SET
                        capital_per_trade = EXCLUDED.capital_per_trade,
                        enabled           = TRUE
                    """,
                    (user_id, strategy_id, capital_per_trade),
                )
            conn.commit()
            conn.close()
            return jsonify({
                'success': True,
                'message': f"Subscribed to {strategy_id} with ₹{capital_per_trade:,} per trade. "
                           "Use /reload_subscribers in Telegram to activate.",
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500

    elif request.method == 'DELETE':
        data = request.get_json(silent=True) or {}
        strategy_id = data.get('strategy_id', '').strip().upper()
        if not strategy_id:
            return jsonify({'error': 'strategy_id required'}), 400
        try:
            conn = _conn()
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE user_strategy_subscriptions SET enabled=FALSE WHERE user_id=%s AND strategy_id=%s",
                    (user_id, strategy_id),
                )
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'message': f"Unsubscribed from {strategy_id}."})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/subscriptions/status', methods=['GET'])
@token_required
def subscription_status(current_user):
    """
    Show this user's broker connection status + active strategy subscriptions.
    """
    import psycopg2 as _pg
    user_id = current_user['id']
    try:
        conn = _pg.connect(
            host=app.config['DB_HOST'], database=app.config['DB_NAME'],
            user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT broker_type, is_active, expires_at FROM user_broker_sessions WHERE user_id=%s",
                (user_id,),
            )
            broker_rows = cur.fetchall()
            cur.execute(
                "SELECT strategy_id, capital_per_trade, enabled FROM user_strategy_subscriptions WHERE user_id=%s",
                (user_id,),
            )
            sub_rows = cur.fetchall()
        conn.close()
        return jsonify({
            'success': True,
            'user_id': user_id,
            'brokers': [
                {
                    'type':      r[0],
                    'active':    r[1],
                    'expires_at': r[2].isoformat() if r[2] else None,
                }
                for r in broker_rows
            ],
            'subscriptions': [
                {
                    'strategy_id':       r[0],
                    'capital_per_trade': r[1],
                    'enabled':           r[2],
                }
                for r in sub_rows
            ],
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# MARKET DATA ENDPOINTS - SIMPLIFIED
# ============================================================================

@app.route('/api/market_data_enhanced', methods=['GET'])
@token_required
def get_market_data(current_user):
    """Get market data from enhanced schema"""
    try:
        symbol = request.args.get('symbol')
        limit = request.args.get('limit', 1000, type=int)
        
        if not symbol:
            return jsonify({'error': 'symbol parameter required'}), 400
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT timestamp, symbol, open, high, low, close, volume
                FROM market_data_enhanced 
                WHERE symbol = %s 
                ORDER BY timestamp DESC LIMIT %s
            """, (symbol, limit))
            
            rows = cursor.fetchall()
            
            data = []
            for row in rows:
                data.append({
                    'timestamp': row[0].isoformat(),
                    'symbol': row[1],
                    'open': float(row[2]) if row[2] else None,
                    'high': float(row[3]) if row[3] else None,
                    'low': float(row[4]) if row[4] else None,
                    'close': float(row[5]) if row[5] else None,
                    'volume': int(row[6]) if row[6] else 0
                })
        
        conn.close()
        
        return jsonify({
            'data': data,
            'count': len(data),
            'symbol': symbol
        })
        
    except Exception as e:
        logger.error(f"Market data error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/latest_prices', methods=['GET'])
@token_required
def get_latest_prices(current_user):
    """Get latest prices"""
    try:
        symbols = request.args.get('symbols', '').split(',')
        symbols = [s.strip() for s in symbols if s.strip()]
        
        if not symbols:
            return jsonify({'error': 'symbols parameter required'}), 400
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        with conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(symbols))
            cursor.execute(f"""
                SELECT DISTINCT ON (symbol) 
                       symbol, close, timestamp
                FROM market_data_enhanced
                WHERE symbol IN ({placeholders})
                ORDER BY symbol, timestamp DESC
            """, symbols)
            
            rows = cursor.fetchall()
            
            prices = {}
            for row in rows:
                symbol, price, timestamp = row
                prices[symbol] = {
                    'symbol': symbol,
                    'price': float(price) if price else 0.0,
                    'timestamp': timestamp.isoformat() if timestamp else None
                }
        
        conn.close()
        
        return jsonify({
            'data': prices,
            'count': len(prices)
        })
        
    except Exception as e:
        logger.error(f"Latest prices error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/indicators/available', methods=['GET'])
@token_required  
def get_available_indicators(current_user):
    """Get available technical indicators"""
    try:
        indicators = [
            {'name': 'SMA', 'description': 'Simple Moving Average', 'periods': [5, 10, 20, 50, 200]},
            {'name': 'EMA', 'description': 'Exponential Moving Average', 'periods': [12, 26, 50]},
            {'name': 'RSI', 'description': 'Relative Strength Index', 'periods': [14]},
            {'name': 'MACD', 'description': 'Moving Average Convergence Divergence', 'periods': [12, 26, 9]},
            {'name': 'Bollinger Bands', 'description': 'Bollinger Bands', 'periods': [20]},
            {'name': 'ATR', 'description': 'Average True Range', 'periods': [14]}
        ]
        
        return jsonify({
            'indicators': indicators,
            'count': len(indicators)
        })
        
    except Exception as e:
        logger.error(f"Get indicators error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/instruments', methods=['GET'])
@token_required
def get_instruments(current_user):
    """Get trading instruments - basic implementation"""
    try:
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)
        search = request.args.get('search', '')
        
        # Basic instruments response - you can enhance this with actual data
        instruments = []
        
        # Try to get from Kite if available
        kite_client = KiteHelper.get_kite_client()
        if kite_client:
            try:
                # Get instruments from Kite
                all_instruments = kite_client.instruments()
                
                # Filter if search term provided
                if search:
                    filtered_instruments = [
                        inst for inst in all_instruments 
                        if search.lower() in inst.get('name', '').lower() or 
                           search.lower() in inst.get('tradingsymbol', '').lower()
                    ]
                else:
                    filtered_instruments = all_instruments
                
                # Apply pagination
                start_idx = offset
                end_idx = offset + limit
                instruments = filtered_instruments[start_idx:end_idx]
                
                return jsonify({
                    'success': True,
                    'instruments': instruments,
                    'count': len(instruments),
                    'total': len(filtered_instruments),
                    'limit': limit,
                    'offset': offset,
                    'source': 'kite_api'
                })
                
            except Exception as e:
                logger.error(f"Kite instruments error: {e}")
        
        # Fallback: basic mock instruments if Kite not available
        mock_instruments = [
            {
                'instrument_token': f'{i+1000}',
                'exchange_token': f'{i+1000}',
                'tradingsymbol': f'STOCK{i+1}',
                'name': f'Stock Company {i+1}',
                'last_price': 100.0 + (i * 10),
                'expiry': '',
                'strike': 0.0,
                'tick_size': 0.05,
                'lot_size': 1,
                'instrument_type': 'EQ',
                'segment': 'NSE',
                'exchange': 'NSE'
            }
            for i in range(offset, min(offset + limit, 50))  # Limit mock data to 50 items
        ]
        
        return jsonify({
            'success': True,
            'instruments': mock_instruments,
            'count': len(mock_instruments),
            'total': 50,
            'limit': limit,
            'offset': offset,
            'source': 'mock_data',
            'message': 'Mock data - authenticate with Kite for real instruments'
        })
        
    except Exception as e:
        logger.error(f"Instruments error: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'instruments': [],
            'count': 0
        }), 500

@app.route('/api/db/stats', methods=['GET'])
@token_required
def database_stats(current_user):
    """Simple database statistics"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        with conn.cursor() as cursor:
            # Table counts
            tables_info = []
            
            for table in ['users', 'api_tokens']:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    tables_info.append({'name': table, 'count': count})
                except Exception as e:
                    tables_info.append({'name': table, 'count': 0, 'error': str(e)})
            
            # Token breakdown by broker type
            try:
                cursor.execute("""
                    SELECT broker_type, COUNT(*) as count,
                           COUNT(CASE WHEN expires_at IS NULL OR expires_at > NOW() THEN 1 END) as active
                    FROM api_tokens 
                    GROUP BY broker_type
                """)
                token_breakdown = []
                for row in cursor.fetchall():
                    token_breakdown.append({
                        'broker_type': row[0],
                        'total_tokens': row[1], 
                        'active_tokens': row[2]
                    })
                tables_info.append({'name': 'token_breakdown', 'breakdown': token_breakdown})
            except Exception as e:
                tables_info.append({'name': 'token_breakdown', 'error': str(e)})
            
            # Database size
            cursor.execute("SELECT pg_database_size(current_database()) as db_size")
            db_size = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'tables': tables_info,
            'database_size_mb': round(db_size / (1024 * 1024), 2),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/login', methods=['GET'])
def login_page():
    """Serve login page"""
    return send_from_directory(app.static_folder, 'login.html')


@app.route('/api/subscribe/signin', methods=['GET'])
def subscribe_signin():
    """
    Server-side redirect to Kite OAuth for subscriber sign-in.
    Registers the client IP so /api/kite/callback can detect the subscribe flow.
    """
    client_ip = request.headers.get('X-Real-IP') or request.remote_addr
    _sub_register_ip(client_ip)
    logger.info(f"[subscribe_signin] IP={client_ip} registered for subscribe flow")
    kite_api_key = app.config.get('KITE_API_KEY', '')
    return redirect(f'https://kite.zerodha.com/connect/login?api_key={kite_api_key}&v=3&state=browse')


_SUBSCRIBE_BREEZE_PENDING_FILE = '/tmp/subscribe_breeze_pending_ips.json'

def _sub_breeze_register_ip(ip: str):
    try:
        try:
            data = _json.loads(open(_SUBSCRIBE_BREEZE_PENDING_FILE).read())
        except Exception:
            data = {}
        data[ip] = _sub_time.time() + 600
        open(_SUBSCRIBE_BREEZE_PENDING_FILE, 'w').write(_json.dumps(data))
    except Exception as e:
        logger.warning(f"[subscribe] _sub_breeze_register_ip failed: {e}")

def _sub_breeze_check_and_consume_ip(ip: str) -> bool:
    try:
        data = _json.loads(open(_SUBSCRIBE_BREEZE_PENDING_FILE).read())
        ts = data.pop(ip, 0)
        open(_SUBSCRIBE_BREEZE_PENDING_FILE, 'w').write(_json.dumps(data))
        return ts > _sub_time.time()
    except Exception:
        return False


@app.route('/api/subscribe/breeze-signin', methods=['GET'])
def subscribe_breeze_signin():
    """
    Server-side redirect to Breeze OAuth for subscriber sign-in.
    Registers client IP so the master /api/breeze/callback can detect the subscribe flow.
    """
    import urllib.parse
    api_key = app.config.get('BREEZE_API_KEY', '')
    if not api_key:
        return redirect('/subscribe?error=breeze_not_configured')
    client_ip = request.headers.get('X-Real-IP') or request.remote_addr
    _sub_breeze_register_ip(client_ip)
    encoded_key = urllib.parse.quote(api_key, safe='')
    logger.info(f"[subscribe_breeze_signin] IP={client_ip} registered for subscribe flow")
    return redirect(f'https://api.icicidirect.com/apiuser/login?api_key={encoded_key}')


@app.route('/subscribe', methods=['GET'])
def subscribe_page():
    """Subscriber onboarding page — public, no login required."""
    import re
    page_path = '/opt/stockapp/aladin/static/subscribe.html'
    with open(page_path, 'r') as f:
        html = f.read()
    # Inject the real Kite API key so the page can build the OAuth URL client-side
    html = html.replace('__KITE_API_KEY__', app.config.get('KITE_API_KEY', ''))
    return html, 200, {
        'Content-Type': 'text/html',
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0',
    }


@app.route('/api/kite/subscribe-callback', methods=['GET'])
def kite_subscribe_callback():
    """
    Kite OAuth callback for copy-trading subscribers.
    - Exchanges request_token for access_token
    - Calls profile() to get DMAT name (e.g. "Rajiv Moolchandani")
    - Creates/updates user row; saves broker session with pending_approval=TRUE, is_active=FALSE
    - Saves strategy subscription (enabled=TRUE; won't activate until session is approved)
    - Notifies admin via Telegram: DMAT name + /approve <user_id>
    - Redirects to /subscribe?pending=1

    This URL must be whitelisted in your Kite Connect app redirect URLs:
      https://alaidin.info/api/kite/subscribe-callback
    """
    try:
        request_token = request.args.get('request_token')
        state         = request.args.get('state', '')

        if not request_token:
            return redirect('/subscribe?error=missing_token')

        # Exchange for access token
        kite = KiteConnect(api_key=app.config['KITE_API_KEY'])
        session_data = kite.generate_session(
            request_token = request_token,
            api_secret    = app.config['KITE_API_SECRET'],
        )
        access_token  = session_data['access_token']
        kite_user_id  = session_data.get('user_id', '')

        # Get DMAT name from profile
        kite.set_access_token(access_token)
        profile_data  = kite.profile()
        dmat_name     = profile_data.get('user_name', kite_user_id)

        # Owner shortcut: WI0733 logging in via subscribe page → save to api_tokens + redirect to dashboard
        try:
            from tradingv2.config import OWNER_KITE_ID
        except Exception:
            OWNER_KITE_ID = 'WI0733'
        if kite_user_id == OWNER_KITE_ID:
            import psycopg2 as _pgown
            conn_own = _pgown.connect(
                host=app.config['DB_HOST'], database=app.config['DB_NAME'],
                user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
                connect_timeout=5,
            )
            with conn_own.cursor() as cur_own:
                cur_own.execute("SELECT id FROM users WHERE username = %s", (kite_user_id,))
                row_own = cur_own.fetchone()
                if row_own:
                    owner_user_id = row_own[0]
                    cur_own.execute("UPDATE users SET role = 'owner' WHERE id = %s", (owner_user_id,))
                else:
                    import hashlib as _hl, secrets as _sc
                    pw_hash = _hl.sha256(_sc.token_bytes(32)).hexdigest()
                    cur_own.execute(
                        "INSERT INTO users (username, password, role) VALUES (%s, %s, 'owner') RETURNING id",
                        (kite_user_id, pw_hash),
                    )
                    owner_user_id = cur_own.fetchone()[0]
                cur_own.execute(
                    """
                    INSERT INTO user_broker_sessions
                        (user_id, broker_type, api_key, session_token, expires_at,
                         is_active, pending_approval)
                    VALUES (%s, 'kite', %s, %s, NOW() + INTERVAL '1 day', TRUE, FALSE)
                    ON CONFLICT (user_id, broker_type) DO UPDATE SET
                        session_token    = EXCLUDED.session_token,
                        api_key          = EXCLUDED.api_key,
                        expires_at       = EXCLUDED.expires_at,
                        is_active        = TRUE,
                        pending_approval = FALSE,
                        created_at       = NOW()
                    """,
                    (owner_user_id, app.config['KITE_API_KEY'], access_token),
                )
                cur_own.execute(
                    "INSERT INTO api_tokens (token, broker_type, created_at) VALUES (%s, 'kite', NOW())",
                    (access_token,),
                )
            conn_own.commit()
            conn_own.close()
            logger.info(f"[subscribe] Owner login detected ({kite_user_id}) — token saved, routing to dashboard")
            import secrets as _sec_own
            dash_token = _sec_own.token_hex(16)
            _dashboard_token_write(dash_token, owner_user_id, dmat_name, kite_user_id, 'owner')
            return _auto_enter_dashboard_page(dash_token)

        import psycopg2 as _pg
        conn = _pg.connect(
            host=app.config['DB_HOST'], database=app.config['DB_NAME'],
            user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            # Find or create user
            cur.execute("SELECT id FROM users WHERE username = %s", (kite_user_id,))
            row = cur.fetchone()
            if row:
                user_id = row[0]
            else:
                import hashlib, secrets
                pw_hash = hashlib.sha256(secrets.token_bytes(32)).hexdigest()
                cur.execute(
                    "INSERT INTO users (username, password, role) VALUES (%s, %s, 'subscriber') RETURNING id",
                    (kite_user_id, pw_hash),
                )
                user_id = cur.fetchone()[0]
                logger.info(f"[subscribe] New user: {dmat_name} ({kite_user_id}) uid={user_id}")

            # Save broker session — pending_approval=TRUE, is_active=FALSE until admin approves.
            # If already approved, preserve approval on re-auth (daily token refresh).
            cur.execute(
                """
                INSERT INTO user_broker_sessions
                    (user_id, broker_type, api_key, session_token, expires_at,
                     is_active, pending_approval)
                VALUES (%s, 'kite', %s, %s, NOW() + INTERVAL '1 day', FALSE, TRUE)
                ON CONFLICT (user_id, broker_type) DO UPDATE SET
                    session_token    = EXCLUDED.session_token,
                    api_key          = EXCLUDED.api_key,
                    expires_at       = EXCLUDED.expires_at,
                    is_active        = CASE WHEN user_broker_sessions.is_active = TRUE
                                               AND user_broker_sessions.pending_approval = FALSE
                                           THEN TRUE ELSE FALSE END,
                    pending_approval = CASE WHEN user_broker_sessions.is_active = TRUE
                                               AND user_broker_sessions.pending_approval = FALSE
                                           THEN FALSE ELSE TRUE END,
                    created_at       = NOW()
                """,
                (user_id, app.config['KITE_API_KEY'], access_token),
            )
        conn.commit()
        conn.close()

        # Check if this is an already-approved subscriber (daily re-auth)
        import psycopg2 as _pg2
        _check = _pg2.connect(
            host=app.config['DB_HOST'], database=app.config['DB_NAME'],
            user=app.config['DB_USER'], password=app.config['DB_PASSWORD'], connect_timeout=5)
        with _check.cursor() as _cur:
            _cur.execute(
                "SELECT is_active FROM user_broker_sessions WHERE user_id=%s AND broker_type='kite'",
                (user_id,))
            _row = _cur.fetchone()
        _check.close()
        _already_active = bool(_row and _row[0])

        # Always set subscriber session cookie for dashboard access
        session['sub_user_id'] = user_id
        session['sub_name']    = dmat_name
        session['sub_kite_id'] = kite_user_id
        session['sub_role']    = 'subscriber'
        session.permanent      = True

        # ── Browse mode (state=browse): broker sign-in from /subscribe.
        #    Registration + pending_approval was already written above —
        #    go straight to the dashboard. Strategy picking now happens
        #    there (My Strategies tab) once the owner approves the account.
        if state == 'browse':
            if _already_active:
                logger.info(f"[subscribe] Re-auth approved subscriber → dashboard: {dmat_name} ({kite_user_id})")
            else:
                logger.info(f"[subscribe] New subscriber registered, pending approval → dashboard: {dmat_name} ({kite_user_id})")
                _notify_admin_new_subscriber(dmat_name, kite_user_id, user_id, '', 0)
            import secrets as _sec_sub
            dash_token = _sec_sub.token_hex(16)
            _dashboard_token_write(dash_token, user_id, dmat_name, kite_user_id, 'subscriber')
            return _auto_enter_dashboard_page(dash_token)

        # ── Legacy / direct mode: strategy_id + capital passed as query params
        strategy_id = request.args.get('strategy_id', '').strip().upper()
        capital     = int(request.args.get('capital', 0) or 0)

        if strategy_id and capital >= 5000:
            with _pg.connect(
                host=app.config['DB_HOST'], database=app.config['DB_NAME'],
                user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
                connect_timeout=5,
            ) as conn2:
                with conn2.cursor() as cur2:
                    cur2.execute(
                        """
                        INSERT INTO user_strategy_subscriptions
                            (user_id, strategy_id, capital_per_trade, enabled)
                        VALUES (%s, %s, %s, TRUE)
                        ON CONFLICT (user_id, strategy_id) DO UPDATE SET
                            capital_per_trade = EXCLUDED.capital_per_trade,
                            enabled = TRUE
                        """,
                        (user_id, strategy_id, capital),
                    )
                conn2.commit()

        _notify_admin_new_subscriber(dmat_name, kite_user_id, user_id, strategy_id, capital)

        logger.info(
            f"[subscribe] Pending approval: {dmat_name} ({kite_user_id}) uid={user_id} "
            f"strategy={strategy_id} capital={capital}"
        )
        from urllib.parse import urlencode
        qs = urlencode({'pending': '1', 'kite_user': dmat_name, 'kite_id': kite_user_id, 'strategy_id': strategy_id, 'capital': capital})
        return redirect(f'/subscribe?{qs}')

    except Exception as e:
        logger.error(f"[subscribe] kite_subscribe_callback error: {e}")
        return redirect(f'/subscribe?error={str(e)[:60]}')


def _notify_admin_new_subscriber(dmat_name, kite_user_id, user_id, strategy_id, capital):
    """Send Telegram notification to admin for a new subscription."""
    _bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    _chat_id   = os.getenv('TELEGRAM_CHAT_ID')
    if not (_bot_token and _chat_id):
        return
    try:
        msg = (
            f"New subscriber request:\n"
            f"DMAT: {dmat_name} | Kite ID: {kite_user_id}\n"
            f"Strategy: {strategy_id or '—'} | Capital: ₹{capital:,}\n"
            f"/approve {user_id}"
        )
        requests.post(
            f"https://api.telegram.org/bot{_bot_token}/sendMessage",
            json={'chat_id': _chat_id, 'text': msg},
            timeout=5,
        )
    except Exception as te:
        logger.warning(f"[subscribe] Telegram notify failed: {te}")


@app.route('/api/subscribe/confirm', methods=['POST'])
def subscribe_confirm():
    """
    Complete subscription after browse-mode OAuth.
    Called from the frontend after the user picks strategies and capitals.
    Body: {sid, strategies: [{strategy_id, capital}, ...]}
      or legacy: {sid, strategy_id, capital}
    Returns: {status: 'pending'|'done', kite_user, strategies, broker} or {error: msg}
    """
    try:
        body        = request.get_json(force=True) or {}
        sid         = (body.get('sid') or '').strip()

        # Normalise to list — accept both multi and legacy single-strategy payloads
        raw_strats = body.get('strategies')
        # Use explicit None/missing check, not `not raw_strats` (which catches empty list)
        if raw_strats is None:
            single_id  = (body.get('strategy_id') or '').strip().upper()
            single_cap = int(body.get('capital') or 0)
            raw_strats = [{'strategy_id': single_id, 'capital': single_cap}] if single_id else []

        strategies = [
            {'strategy_id': s['strategy_id'].strip().upper(), 'capital': int(s.get('capital') or 0)}
            for s in raw_strats if s.get('strategy_id')
        ]
        logger.info(f"[subscribe/confirm] sid={sid[:8] if sid else 'none'}… raw_strats={len(raw_strats)} strategies={len(strategies)}")

        if not sid:
            return jsonify({'error': 'Missing session — please sign in again.'}), 400
        if not strategies:
            return jsonify({'error': 'No strategy selected.'}), 400
        for s in strategies:
            if s['capital'] < 5000:
                return jsonify({'error': f"Minimum capital is ₹5,000 (got ₹{s['capital']:,} for {s['strategy_id']})."}), 400

        sess = _browse_session_consume(sid)
        if not sess:
            return jsonify({'error': 'Session expired or invalid — please sign in again.'}), 400
        user_id      = sess['user_id']
        kite_user_id = sess['kite_user_id']
        dmat_name    = sess['dmat_name']
        broker_type  = sess.get('broker_type', 'kite')

        # Reinstate Flask session so "Go to Dashboard" works even if the original
        # OAuth session cookie expired (browser restart, 24h limit, different tab).
        session['sub_user_id'] = user_id
        session['sub_name']    = dmat_name
        session['sub_kite_id'] = kite_user_id
        if session.get('sub_role') != 'owner':
            session['sub_role'] = 'subscriber'
        session.permanent      = True

        import psycopg2 as _pg
        conn = _pg.connect(
            host=app.config['DB_HOST'], database=app.config['DB_NAME'],
            user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            for s in strategies:
                cur.execute(
                    """
                    INSERT INTO user_strategy_subscriptions
                        (user_id, strategy_id, capital_per_trade, enabled)
                    VALUES (%s, %s, %s, TRUE)
                    ON CONFLICT (user_id, strategy_id) DO UPDATE SET
                        capital_per_trade = EXCLUDED.capital_per_trade,
                        enabled = TRUE
                    """,
                    (user_id, s['strategy_id'], s['capital']),
                )
            # Disable any previously subscribed strategies not in this selection
            selected_ids = [s['strategy_id'] for s in strategies]
            placeholders = ','.join(['%s'] * len(selected_ids))
            cur.execute(
                f"UPDATE user_strategy_subscriptions SET enabled = FALSE "
                f"WHERE user_id = %s AND strategy_id NOT IN ({placeholders})",
                [user_id] + selected_ids,
            )
        conn.commit()
        conn.close()

        # Backwards-compat single values for response/logging
        strategy_id = strategies[0]['strategy_id']
        capital     = strategies[0]['capital']

        strats_summary = ', '.join(f"{s['strategy_id']}@₹{s['capital']:,}" for s in strategies)
        logger.info(
            f"[subscribe] Subscription confirmed ({broker_type}): {dmat_name} ({kite_user_id}) uid={user_id} "
            f"strategies=[{strats_summary}]"
        )

        strats_response = [{'strategy_id': s['strategy_id'], 'capital': s['capital']} for s in strategies]

        if broker_type == 'breeze':
            try:
                open('/tmp/subscriber_reload_requested', 'w').write('1')
            except Exception:
                pass
            import secrets as _btok_b
            _btok_b_val = _btok_b.token_hex(16)
            _dashboard_token_write(_btok_b_val, user_id, dmat_name, kite_user_id, 'subscriber')
            _notify_admin_new_subscriber(dmat_name, kite_user_id, user_id, strategy_id, capital)
            return jsonify({'status': 'done', 'kite_user': dmat_name,
                            'strategies': strats_response, 'broker': 'breeze',
                            'strategy_id': strategy_id, 'capital': capital,
                            'dashboard_token': _btok_b_val})

        # Owner is auto-approved — activate session immediately
        try:
            from tradingv2.config import OWNER_KITE_ID as _OKID
        except Exception:
            _OKID = 'WI0733'
        if kite_user_id == _OKID or session.get('sub_role') == 'owner':
            import psycopg2 as _pg_oa
            _conn_oa = _pg_oa.connect(
                host=app.config['DB_HOST'], database=app.config['DB_NAME'],
                user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
                connect_timeout=5,
            )
            with _conn_oa.cursor() as _cur_oa:
                _cur_oa.execute(
                    "UPDATE user_broker_sessions SET is_active=TRUE, pending_approval=FALSE "
                    "WHERE user_id=%s AND broker_type='kite'",
                    (user_id,),
                )
            _conn_oa.commit()
            _conn_oa.close()
            try:
                open('/tmp/subscriber_reload_requested', 'w').write('1')
            except Exception:
                pass
            import secrets as _btok_k
            _btok_k_val = _btok_k.token_hex(16)
            _dashboard_token_write(_btok_k_val, user_id, dmat_name, kite_user_id, 'owner')
            logger.info(f"[subscribe] Owner auto-approved: {dmat_name} ({kite_user_id}) strategies=[{strats_summary}]")
            return jsonify({'status': 'done', 'kite_user': dmat_name,
                            'strategies': strats_response, 'broker': 'kite',
                            'strategy_id': strategy_id, 'capital': capital,
                            'dashboard_token': _btok_k_val})

        _notify_admin_new_subscriber(dmat_name, kite_user_id, user_id, strategy_id, capital)
        return jsonify({'status': 'pending', 'kite_user': dmat_name,
                        'strategies': strats_response,
                        'strategy_id': strategy_id, 'capital': capital})

    except Exception as e:
        logger.error(f"[subscribe] confirm error: {e}")
        return jsonify({'error': 'Server error — try again.'}), 500


@app.route('/api/subscribe/status', methods=['GET'])
def subscribe_status():
    """
    Poll endpoint for the pending page.
    GET /api/subscribe/status?kite_id=WI0733
    Returns: {status: 'approved'} or {status: 'pending'} or {error: ...}
    """
    kite_id = (request.args.get('kite_id') or '').strip().upper()
    if not kite_id:
        return jsonify({'error': 'missing kite_id'}), 400
    try:
        import psycopg2 as _pg
        conn = _pg.connect(
            host=app.config['DB_HOST'], database=app.config['DB_NAME'],
            user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ubs.is_active, u.id, u.username
                FROM user_broker_sessions ubs
                JOIN users u ON u.id = ubs.user_id
                WHERE u.username = %s AND ubs.broker_type = 'kite'
                """,
                (kite_id,),
            )
            row = cur.fetchone()
        conn.close()
        if row is None:
            return jsonify({'status': 'pending'})
        is_active, user_id, username = row
        if not is_active:
            return jsonify({'status': 'pending'})
        # Generate a one-time dashboard token so the JS can navigate to /dashboard
        import secrets as _stok_s
        dash_token = _stok_s.token_hex(16)
        _dashboard_token_write(dash_token, user_id, kite_id, kite_id, 'subscriber')
        return jsonify({'status': 'approved', 'dashboard_token': dash_token})
    except Exception as e:
        logger.error(f"[subscribe/status] error: {e}")
        return jsonify({'error': 'server error'}), 500


@app.route('/api/strategies/live-stats', methods=['GET'])
def strategies_live_stats():
    """
    Return per-strategy live P&L summary from the last 90 days of closed trades.
    Used by the subscribe page to show real avg/trade alongside backtest numbers.
    Returns: {STRATEGY_ID: {avg_pnl: int, trades: int}, ...}
    """
    try:
        import psycopg2 as _pg
        conn = _pg.connect(
            host=app.config['DB_HOST'], database=app.config['DB_NAME'],
            user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute("""
                SELECT strategy_id,
                       ROUND(AVG(pnl_rupees)::numeric, 0) AS avg_pnl,
                       COUNT(*) AS trade_count
                FROM trades_v2
                WHERE exit_time IS NOT NULL
                  AND exit_time >= NOW() - INTERVAL '90 days'
                  AND pnl_rupees IS NOT NULL
                GROUP BY strategy_id
            """)
            rows = cur.fetchall()
        conn.close()
        result = {}
        for strategy_id, avg_pnl, trade_count in rows:
            result[strategy_id] = {
                'avg_pnl': int(avg_pnl) if avg_pnl is not None else 0,
                'trades':  int(trade_count),
            }
        return jsonify(result)
    except Exception as e:
        logger.error(f"[live_stats] error: {e}")
        return jsonify({}), 200   # return empty dict — frontend shows '—' gracefully


_STRATEGY_CHAT_SYSTEM_PROMPT = """You are a friendly trading strategy consultant helping retail investors document their trading ideas in plain, simple English.
No jargon. Your job is to understand their idea across 6 dimensions, one at a time:
1. ENTRY: What event or signal triggers the trade? (gap at open, volume spike, chart pattern, news?)
2. DIRECTION: Buy (Long) or Sell Short?
3. RISK: Where to cut losses? (% below entry, VWAP, prior high?)
4. TARGET: Where to take profit?
5. TIMING: Which market hours? How long to hold?
6. UNIVERSE: Any stock preference? (large-cap, price above ₹X, specific sectors?)

Rules:
- Ask ONE dimension at a time. Keep replies to 2-3 sentences. Be warm and encouraging.
- When you have enough info on all 6 dimensions, include a structured summary in your reply.
- Embed the summary between <<<JSON>>> and <<<END>>> markers — do not put it on a separate line.
- Summary JSON keys: entry, direction, risk, target, timing, universe, summary_english.
- Example: Great, I have everything I need! <<<JSON>>>{"entry":"gap up >2% at open","direction":"Long","risk":"-1% below entry","target":"+2% initial","timing":"09:15-15:00 IST","universe":"NSE liquid stocks >₹20","summary_english":"Buy stocks gapping up >2% at open, target +2%, stop -1% below entry."}<<<END>>>"""


@app.route('/api/subscribe/chat', methods=['POST'])
def subscribe_chat():
    """
    DeepSeek-powered strategy builder chat (multi-turn).
    Body: {messages: [{role: 'user'|'assistant', content: str}]}
    Returns: {reply: str, done: bool, strategy_json: obj|null}
    """
    try:
        body     = request.get_json(force=True) or {}
        messages = body.get('messages', [])

        if not messages:
            return jsonify({'error': 'No messages provided'}), 400

        # Sanitize: only user/assistant roles, cap content, cap history to 20 turns
        clean_msgs = [
            {'role': m['role'], 'content': str(m.get('content', ''))[:1000]}
            for m in messages
            if m.get('role') in ('user', 'assistant') and m.get('content')
        ][-20:]

        api_key = os.getenv('DEEPSEEK_API_KEY')
        if not api_key:
            return jsonify({'reply': 'Chat is temporarily unavailable.', 'done': False, 'strategy_json': None})

        payload = {
            'model': 'deepseek-chat',
            'messages': [
                {'role': 'system', 'content': _STRATEGY_CHAT_SYSTEM_PROMPT},
                *clean_msgs,
            ],
            'temperature': 0.7,
            'max_tokens': 400,
        }
        resp = requests.post(
            'https://api.deepseek.com/v1/chat/completions',
            json=payload,
            headers={
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json',
            },
            timeout=8,
        )
        resp.raise_for_status()
        reply_text = resp.json()['choices'][0]['message']['content']

        # Extract structured summary if embedded
        done = False
        strategy_json = None
        if '<<<JSON>>>' in reply_text and '<<<END>>>' in reply_text:
            try:
                import json as _json
                raw = reply_text.split('<<<JSON>>>')[1].split('<<<END>>>')[0].strip()
                strategy_json = _json.loads(raw)
                done = True
                reply_text = reply_text.split('<<<JSON>>>')[0].strip()
            except Exception:
                pass  # If parse fails, return raw text

        return jsonify({'reply': reply_text, 'done': done, 'strategy_json': strategy_json})

    except requests.Timeout:
        logger.warning('[subscribe_chat] DeepSeek timeout')
        return jsonify({'reply': "Taking a moment — please try again.", 'done': False, 'strategy_json': None})
    except Exception as e:
        logger.error(f'[subscribe_chat] error: {e}')
        return jsonify({'error': 'Server error'}), 500


@app.route('/api/subscribe/chat/submit', methods=['POST'])
def subscribe_chat_submit():
    """
    Store a strategy proposal from the chat interface and notify supervisor.
    Body: {sid, messages, strategy_json, user_name}
    Returns: {status: 'received'}
    """
    try:
        import psycopg2 as _pg, json as _json
        body          = request.get_json(force=True) or {}
        sid           = (body.get('sid') or '').strip()
        messages      = body.get('messages', [])
        strategy_json = body.get('strategy_json')
        user_name     = (body.get('user_name') or 'Anonymous')[:100]

        if not messages:
            return jsonify({'error': 'No conversation provided'}), 400

        # Resolve user_id from browse session (optional — user_id is nullable)
        user_id = None
        if sid:
            sess = _browse_session_get(sid)
            if sess:
                user_id = sess.get('user_id')

        conn = _pg.connect(
            host=app.config['DB_HOST'], database=app.config['DB_NAME'],
            user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO subscriber_strategy_proposals
                    (user_id, user_name, messages, strategy_json)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    user_id,
                    user_name,
                    _json.dumps(messages),
                    _json.dumps(strategy_json) if strategy_json else None,
                ),
            )
        conn.commit()
        conn.close()

        # Notify supervisor
        summary = (strategy_json or {}).get('summary_english', '(conversation submitted, no summary yet)')
        _notify_supervisor_strategy_proposal(user_name, summary)

        logger.info(f"[subscribe_chat_submit] proposal from '{user_name}' (user_id={user_id})")
        return jsonify({'status': 'received'})

    except Exception as e:
        logger.error(f'[subscribe_chat_submit] error: {e}')
        return jsonify({'error': 'Server error'}), 500


def _browse_session_get(sid: str):
    """Non-consuming read of a browse session (does not delete it)."""
    try:
        import json as _json, time as _time
        path = '/tmp/subscribe_browse_sessions.json'
        if not os.path.exists(path):
            return None
        with open(path, 'r') as f:
            sessions = _json.load(f)
        sess = sessions.get(sid)
        if not sess:
            return None
        if _time.time() > sess.get('expires_timestamp', 0):
            return None   # expired
        return sess
    except Exception:
        return None


@app.route('/api/subscribe/enter-dashboard', methods=['GET'])
def subscribe_enter_dashboard():
    """
    One-time auth redirect: exchanges a dashboard_token (from subscribe_confirm JSON)
    for a real Flask session, then redirects to /dashboard.
    Bypasses the Flask cookie-from-fetch reliability issue after OAuth redirects.
    """
    token = request.args.get('token', '').strip()
    if not token:
        logger.warning("[enter-dashboard] missing token")
        return redirect('/subscribe?error=missing_token')
    info = _dashboard_token_consume(token)
    if not info:
        logger.warning(f"[enter-dashboard] token not found or expired: {token[:8]}…")
        return redirect('/subscribe?error=token_expired')
    session['sub_user_id'] = info['user_id']
    session['sub_name']    = info['user_name']
    session['sub_kite_id'] = info['kite_id']
    session['sub_role']    = info['role']
    session.permanent      = True
    logger.info(
        f"[enter-dashboard] session set uid={info['user_id']} role={info['role']} "
        f"→ redirecting to /dashboard"
    )
    return redirect('/dashboard')


def _notify_supervisor_strategy_proposal(user_name: str, summary_english: str):
    """Notify the supervisor channel about a new strategy idea submission."""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id   = os.getenv('TELEGRAM_CHAT_ID')
    if not (bot_token and chat_id):
        return
    try:
        msg = (
            f"\U0001F4A1 New strategy idea from {user_name}:\n"
            f"{summary_english}"
        )
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={'chat_id': chat_id, 'text': msg},
            timeout=5,
        )
    except Exception as te:
        logger.warning(f"[subscribe] Telegram proposal notify failed: {te}")


@app.route('/api/auth/breeze/subscribe-url', methods=['GET'])
def breeze_subscribe_url():
    """
    Return the Breeze OAuth URL for subscriber onboarding.
    strategy_id and capital are passed as query params; they'll survive as the
    page uses localStorage to persist them across the OAuth redirect.
    """
    import urllib.parse
    strategy_id = request.args.get('strategy_id', '')
    capital     = request.args.get('capital', '0')
    api_key     = app.config.get('BREEZE_API_KEY', '')
    if not api_key:
        return jsonify({'error': 'Breeze API key not configured'}), 500
    encoded_key = urllib.parse.quote(api_key, safe='')
    # Breeze callback is separate from the master callback
    callback = (
        request.host_url.rstrip('/') +
        f'/api/breeze/subscribe-callback?strategy_id={strategy_id}&capital={capital}'
    )
    auth_url = f"https://api.icicidirect.com/apiuser/login?api_key={encoded_key}"
    return jsonify({'auth_url': auth_url, 'callback_url': callback})


@app.route('/api/breeze/subscribe-callback', methods=['GET', 'POST'])
def breeze_subscribe_callback():
    """Breeze OAuth callback for copy-trading subscribers."""
    try:
        if request.method == 'POST':
            data = request.get_json(silent=True) or request.form
        else:
            data = request.args
        api_session = (data.get('api_session') or data.get('apisession')
                       or request.args.get('api_session') or request.args.get('apisession')
                       or data.get('token'))
        strategy_id = (data.get('strategy_id') or request.args.get('strategy_id', '')).upper()
        capital     = int(data.get('capital') or request.args.get('capital', 0) or 0)

        if not api_session:
            return redirect('/subscribe?error=missing_session')

        # Get Breeze profile to identify the user
        from breeze_connect import BreezeConnect
        breeze = BreezeConnect(api_key=app.config.get('BREEZE_API_KEY', ''))
        breeze.generate_session(api_secret=app.config.get('BREEZE_API_SECRET', ''), session_token=api_session)
        profile_resp = breeze.get_customer_details(api_session=api_session)
        profile = profile_resp.get('Success') or {}
        if isinstance(profile, list) and profile:
            profile = profile[0]
        breeze_user_id = (profile.get('idirect_userid') or profile.get('customer_id')
                          or profile.get('idirect_user_id') or '')
        if not breeze_user_id:
            logger.error(f"[breeze_subscribe_callback] Could not extract user ID from profile: {profile_resp}")
            return redirect('/subscribe?error=breeze_profile_failed')
        breeze_name = profile.get('idirect_user_name') or profile.get('customer_name') or breeze_user_id

        import psycopg2 as _pg, hashlib, secrets
        conn = _pg.connect(
            host=app.config['DB_HOST'], database=app.config['DB_NAME'],
            user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            breeze_username = f'breeze_{breeze_user_id}'
            cur.execute("SELECT id FROM users WHERE username = %s", (breeze_username,))
            row = cur.fetchone()
            if row:
                user_id = row[0]
            else:
                pw_hash = hashlib.sha256(secrets.token_bytes(32)).hexdigest()
                cur.execute(
                    "INSERT INTO users (username, password, role) VALUES (%s, %s, 'subscriber') RETURNING id",
                    (breeze_username, pw_hash),
                )
                user_id = cur.fetchone()[0]
                logger.info(f"[subscribe] New Breeze subscriber: {breeze_user_id} uid={user_id}")

            cur.execute(
                """
                INSERT INTO user_broker_sessions
                    (user_id, broker_type, api_key, session_token, expires_at, is_active)
                VALUES (%s, 'breeze', %s, %s, NOW() + INTERVAL '1 day', TRUE)
                ON CONFLICT (user_id, broker_type) DO UPDATE SET
                    session_token = EXCLUDED.session_token,
                    api_key       = EXCLUDED.api_key,
                    expires_at    = EXCLUDED.expires_at,
                    is_active     = TRUE,
                    created_at    = NOW()
                """,
                (user_id, app.config.get('BREEZE_API_KEY', ''), api_session),
            )
            if strategy_id and capital >= 5000:
                cur.execute(
                    """
                    INSERT INTO user_strategy_subscriptions
                        (user_id, strategy_id, capital_per_trade, enabled)
                    VALUES (%s, %s, %s, TRUE)
                    ON CONFLICT (user_id, strategy_id) DO UPDATE SET
                        capital_per_trade = EXCLUDED.capital_per_trade,
                        enabled = TRUE
                    """,
                    (user_id, strategy_id, capital),
                )
        conn.commit()
        conn.close()

        # Always set subscriber session cookie for dashboard access (Breeze auto-approved)
        session['sub_user_id'] = user_id
        session['sub_name']    = breeze_name
        session['sub_kite_id'] = f'breeze_{breeze_user_id}'
        session['sub_role']    = 'subscriber'
        session.permanent      = True

        from urllib.parse import urlencode
        # Browse mode: no strategy selected yet — go straight to the dashboard;
        # strategy picking happens there (My Strategies tab).
        if not strategy_id:
            logger.info(f"[subscribe] Breeze subscriber connected, no strategy picked → dashboard: {breeze_name} ({breeze_user_id})")
            import secrets as _sec_bz
            dash_token = _sec_bz.token_hex(16)
            _dashboard_token_write(dash_token, user_id, breeze_name, f'breeze_{breeze_user_id}', 'subscriber')
            return _auto_enter_dashboard_page(dash_token)

        logger.info(
            f"[subscribe] Breeze subscriber connected: {breeze_user_id} uid={user_id} "
            f"strategy={strategy_id} capital={capital}"
        )
        qs = urlencode({'done': '1', 'broker': 'breeze', 'kite_user': breeze_name})
        return redirect(f'/subscribe?{qs}')

    except Exception as e:
        logger.error(f"[subscribe] breeze_subscribe_callback error: {e}")
        return redirect(f'/subscribe?error={str(e)[:60]}')


@app.route('/api/neo/subscribe-login', methods=['POST'])
def neo_subscribe_login():
    """Direct-credential Kotak Neo login for a subscriber. Single request:
    performs totp_login() then totp_validate() back-to-back on one NeoAPI
    client instance — unlike Kite/Breeze this has no OAuth redirect to land
    on, so (unlike breeze_subscribe_callback above) it sets the subscriber
    session directly and returns a plain redirect for the page's own JS to
    follow, the same way /api/observer/verify-otp does — no token-relay
    indirection needed since this is reached via same-page fetch(), not an
    external OAuth callback chain.
    mpin is forwarded to Kotak and discarded; never logged or persisted."""
    if not app.config.get('NEO_CONSUMER_KEY'):
        return jsonify({'error': 'Kotak Neo login not yet configured'}), 500

    body   = request.get_json(force=True) or {}
    mobile = (body.get('mobile_number') or '').strip()
    ucc    = (body.get('ucc') or '').strip().upper()
    totp   = (body.get('totp') or '').strip()
    mpin   = body.get('mpin') or ''
    if not (mobile and ucc and totp and mpin):
        return jsonify({'error': 'All fields are required'}), 400

    try:
        from neo_api_client import NeoAPI   # lazy import, mirrors breeze_connect's
                                             # pattern above — package is not in
                                             # requirements.txt yet
    except ImportError:
        return jsonify({'error': 'Kotak Neo integration not installed'}), 500

    try:
        client = NeoAPI(environment='prod', consumer_key=app.config['NEO_CONSUMER_KEY'])
        client.totp_login(mobile_number=mobile, ucc=ucc, totp=totp)
        client.totp_validate(mpin=mpin)
        # mpin is now out of scope for the rest of this function — never
        # reference `mpin` again below this line (including in exception
        # handlers or log lines).
    except Exception as e:
        # Deliberately not logging str(e) — the Neo SDK isn't installed yet so
        # its exception messages are unverified; a message that echoed the
        # mpin back would leak it into logs. Log only the exception type.
        logger.warning(f"[neo-login] auth failed for ucc={ucc} ({type(e).__name__})")
        return jsonify({'error': 'Kotak Neo login failed — check your details'}), 401

    # TODO at implementation time, once neo_api_client is actually installed:
    # confirm the exact attribute the SDK exposes for the resulting session
    # token after totp_validate() succeeds (e.g. client.access_token / a
    # returned dict — not documented in the SDK's public README excerpts we
    # have). Whatever it is, assign it to session_token below.
    session_token = getattr(client, 'access_token', '') or ''
    if not session_token:
        # Fail closed rather than writing a credential-less session that
        # would look connected but silently can't place orders — see
        # NeoBroker's fail-closed posture (execution/neo_broker.py).
        logger.error("[neo-login] auth appeared to succeed but no session token "
                      "was extracted — SDK attribute name needs confirming")
        return jsonify({'error': 'Kotak Neo login incomplete — please contact support'}), 500

    import secrets
    sys.path.insert(0, '/opt/stockapp/tradingv2')
    from config import OWNER_NEO_ID
    is_owner = bool(OWNER_NEO_ID) and ucc == OWNER_NEO_ID
    username = ucc if is_owner else f'neo_{ucc}'  # owner keeps a bare/raw-ID
        # username for consistency with OWNER_KITE_ID's convention; subscriber
        # usernames get the broker-prefixed convention Breeze established
        # (f'breeze_{id}') so a Neo UCC can never collide with a Kite client ID
        # in the users.username UNIQUE constraint.

    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT id, role FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        if row:
            user_id = row[0]
            if is_owner and row[1] != 'owner':
                cur.execute("UPDATE users SET role = 'owner' WHERE id = %s", (user_id,))
        else:
            pw_hash = hashlib.sha256(secrets.token_bytes(32)).hexdigest()
            cur.execute(
                "INSERT INTO users (username, password, role) VALUES (%s, %s, %s) RETURNING id",
                (username, pw_hash, 'owner' if is_owner else 'subscriber'),
            )
            user_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO user_broker_sessions
                (user_id, broker_type, api_key, session_token, expires_at,
                 is_active, pending_approval)
            VALUES (%s, 'neo', %s, %s, NOW() + INTERVAL '1 day', %s, %s)
            ON CONFLICT (user_id, broker_type) DO UPDATE SET
                session_token = EXCLUDED.session_token,
                expires_at    = EXCLUDED.expires_at,
                is_active     = EXCLUDED.is_active,
                pending_approval = EXCLUDED.pending_approval,
                created_at    = NOW()
            """,
            (user_id, app.config['NEO_CONSUMER_KEY'], session_token,
             True if is_owner else False,
             False if is_owner else True),   # pending_approval: Kite-style
                                              # conservative default — Neo is a
                                              # brand-new, untested integration
        )
        if is_owner:
            # Every other api_tokens reader in the codebase filters explicitly
            # on broker_type='kite' or 'breeze' (verified: pre_market_check.py,
            # ws_adapter.py, monitor_service.py, kite_eod_snapshot.py,
            # download_nifty_indices.py, app2.py's own get_kite_token/
            # get_breeze_token) — a 'neo' row here is inert plumbing, not a
            # live risk to the owner's Kite session, until a Neo-aware reader
            # is written.
            cur.execute(
                "INSERT INTO api_tokens (token, broker_type, created_at) VALUES (%s, 'neo', NOW())",
                (session_token,),
            )
    conn.commit()
    return_db_connection(conn)

    session['sub_user_id'] = user_id
    session['sub_name']    = username
    session['sub_kite_id'] = username  # field name is a Kite-first legacy,
        # already reused this way for Breeze — keep using it for Neo too so
        # the rest of app2.py's existing role/id-reading code needs no changes
    session['sub_role']    = 'owner' if is_owner else 'subscriber'
    session.permanent = True

    if not is_owner:
        _notify_admin_new_subscriber(dmat_name=username, kite_user_id=username,
                                      user_id=user_id, strategy_id=None, capital=0)

    logger.info(f"[neo-login] {'Owner' if is_owner else 'Subscriber'} connected via Kotak Neo: "
                f"ucc={ucc} uid={user_id}")
    return jsonify({'status': 'ok', 'redirect': '/dashboard'})


# Add this route to your existing app2.py

# ========================================================================
# PATCH FOR app2.py - Add Missing Fields to Analysis Endpoint
# ========================================================================
# 
# LOCATION: Find the endpoint @app.route('/api/aladin/analysis/<symbol>/latest')
# This should be around line 1200-1300 in app2.py
#
# REPLACE the SELECT statement with this updated version:
# ========================================================================

# ========================================================================
# COMPLETE PATCH: app2.py - Research Report with ALL Fields
# ========================================================================
# REPLACE the entire @app.route('/api/aladin/analysis/<symbol>/latest') endpoint
# ========================================================================

@app.route('/api/aladin/analysis/<symbol>/latest', methods=['GET'])
def get_latest_analysis(symbol):
    """Fetch latest analysis with chart visualization data"""
    try:
        query = """
            SELECT
                id,
                symbol,
                timeframe,
                target_timeframe,
                current_price,
                trend_direction,
                action,
                conviction,
                executive_summary,
                detailed_analysis,
                timeframe_predictions,
                trading_recommendations,
                support_levels,
                resistance_levels,
                entry_points,
                targets,
                stop_loss,
                floor_ceiling_levels,
                indicator_signals,
                risk_metrics,
                timeframe_outlook,
                chart_visualization_data,
                requested_indicators,
                available_indicators,
                original_user_query,
                processing_time,
                data_points_analyzed,
                market_context,
                analysis_timestamp
            FROM analysis_results
            WHERE symbol = %s
            ORDER BY analysis_timestamp DESC
            LIMIT 1
        """

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (symbol.upper(),))
                result = cursor.fetchone()

                if not result:
                    return jsonify({
                        'success': False,
                        'error': f'No analysis found for {symbol}',
                        'data': None
                    })

                # Convert result to dict and handle JSON fields
                analysis_data = dict(result)

                # Ensure timestamp is serializable
                if analysis_data.get('analysis_timestamp'):
                    analysis_data['analysis_timestamp'] = analysis_data['analysis_timestamp'].isoformat()

                return jsonify({
                    'success': True,
                    'data': analysis_data,
                    'symbol': symbol.upper(),
                    'timestamp': analysis_data.get('analysis_timestamp')
                })

    except Exception as e:
        print(f"Error fetching analysis for {symbol}: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Database error: {str(e)}',
            'data': None
        }), 500

@app.route('/api/aladin/chart/<symbol>/ohlcv', methods=['GET'])
def get_chart_ohlcv_data(symbol):
    """FIXED: Get OHLCV data for chart rendering - corrected field mapping"""
    try:
        days = request.args.get('days', 30, type=int)
        
        query = """
            SELECT 
                timestamp,
                open,          -- Confirmed: database uses 'open' not 'open_price'
                high,          -- Confirmed: database uses 'high' not 'high_price'  
                low,           -- Confirmed: database uses 'low' not 'low_price'
                close,         -- Confirmed: database uses 'close' not 'close_price'
                volume
            FROM market_data_enhanced
            WHERE symbol = %s
            AND timestamp >= NOW() - INTERVAL '%s days'
            ORDER BY timestamp ASC
        """
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database unavailable'}), 500
            
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (symbol.upper(), days))
            results = cursor.fetchall()
            
            if not results:
                return jsonify({
                    'success': False,
                    'error': f'No OHLCV data found for {symbol}',
                    'data': []
                })
            
            # Convert to list of dicts with proper data types
            chart_data = []
            for row in results:
                chart_data.append({
                    'timestamp': row['timestamp'].isoformat(),
                    'open': float(row['open']) if row['open'] is not None else 0,
                    'high': float(row['high']) if row['high'] is not None else 0,
                    'low': float(row['low']) if row['low'] is not None else 0,
                    'close': float(row['close']) if row['close'] is not None else 0,
                    'volume': int(row['volume']) if row['volume'] is not None else 0
                })
            
            return jsonify({
                'success': True,
                'symbol': symbol.upper(),
                'data': chart_data,
                'data_points': len(chart_data),
                'timeframe': f'{days} days'
            })
                
    except Exception as e:
        logger.error(f"OHLCV Error for {symbol}: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'data': []
        }), 500


@app.route('/api/aladin/chart/latest/<symbol>', methods=['GET'])
def get_latest_chart(symbol):
    """Get latest chart for symbol"""
    try:
        import os
        from datetime import datetime

        chart_dir = '/opt/stockapp/stockmart/chart_prototypes'
        charts = [f for f in os.listdir(chart_dir)
                 if f.startswith(f"{symbol}_") and f.endswith('.png')]

        if not charts:
            return jsonify({'success': False, 'error': 'No charts found'}), 404

        latest_chart = sorted(charts, reverse=True)[0]
        chart_path = os.path.join(chart_dir, latest_chart)
        file_size = os.path.getsize(chart_path) / 1024  # KB

        return jsonify({
            'success': True,
            'chart_url': f"/api/aladin/chart/{symbol}/{latest_chart}",
            'filename': latest_chart,
            'metadata': {
                'created_at': datetime.now().isoformat(),
                'file_size_kb': file_size
            }
        })
    except Exception as e:
        logger.error(f"Latest chart fetch error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
        
@app.route('/api/aladin/chart/<symbol>/<filename>', methods=['GET'])
def get_chart_image(symbol, filename):
    """
    Serve generated chart image
    
    GET /api/aladin/chart/GODIGIT/GODIGIT_20251012_050135.png
    
    Returns: PNG image file
    """
    try:
        chart_path = f"/opt/stockapp/stockmart/chart_prototypes/{filename}"
        
        if not os.path.exists(chart_path):
            return jsonify({'error': 'Chart not found'}), 404
        
        return send_file(chart_path, mimetype='image/png')
        
    except Exception as e:
        logger.error(f"Chart serving error: {e}")
        return jsonify({'error': str(e)}), 500


# ============================================================================
# INITIALIZE ALADIN DATA ACCESS (Add after class definition)
# ============================================================================

# Initialize ALADIN database access layer
aladin_db = ALADINDataAccess()

# ============================================================================
# ALADIN FLASK ROUTES (Add to app2.py after existing routes)
# ============================================================================

@app.route('/chat')
def chat_page():
    """ALADIN chat interface."""
    return send_from_directory('/opt/stockapp/aladin/static', 'chat.html')


@app.route('/aladin')
def aladin_interface():
    try:
        with open('/opt/stockapp/aladin/templates/aladin_db2.html', 'r') as f:
            return f.read()
    except Exception as e:
        return f"Template error: {e}", 500



# =============================================================================
# PATCH FOR app2.py - Add this to your existing Flask app
# Add these functions after your existing ALADIN routes
# =============================================================================

@app.route('/api/aladin/chat', methods=['POST'])
def aladin_chat():
    """
    FIXED: Proper Server-Sent Events streaming without buffering
    """
    try:
        data = request.json
        user_message = data.get('message', '').strip()
        session_id = data.get('session_id')
        
        if not user_message:
            return jsonify({'error': 'Message required'}), 400
        
        if not session_id:
            session_id = f"aladin_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        
        if not hasattr(app, 'interactive_agent'):
            return jsonify({'error': 'Interactive agent not available'}), 503
        
        logger.info(f"📩 ALADIN chat: {user_message}")
        
        # Process message
        result = app.interactive_agent.process_web_message(user_message, session_id)
        
        # Check if streaming
        if hasattr(result, '__iter__') and not isinstance(result, (str, dict, list)):
            logger.info(f"🌊 Streaming response for: {user_message}")
            
            def generate_sse_stream():
                """
                Generate Server-Sent Events with IMMEDIATE flushing
                This is the key fix!
                """
                try:
                    for update in result:
                        if isinstance(update, dict):
                            # Format as SSE
                            sse_data = f"data: {json.dumps(update)}\n\n"
                            
                            # CRITICAL: Yield AND flush immediately
                            yield sse_data
                            
                            # Force Flask to send immediately (no buffering)
                            import sys
                            sys.stdout.flush()
                        
                        # Small delay to prevent overwhelming client
                        time.sleep(0.01)
                    
                    # Send final done event
                    yield f"data: {json.dumps({'type': 'stream_end', 'session_id': session_id})}\n\n"
                    
                except Exception as e:
                    logger.error(f"Stream error: {e}")
                    error_update = {
                        'type': 'error',
                        'message': f"Stream error: {str(e)}",
                        'session_id': session_id
                    }
                    yield f"data: {json.dumps(error_update)}\n\n"
            
            # Return SSE response with NO BUFFERING headers
            return Response(
                generate_sse_stream(),
                mimetype='text/event-stream',
                headers={
                    'Cache-Control': 'no-cache, no-transform',
                    'Connection': 'keep-alive',
                    'X-Accel-Buffering': 'no',  # Disable nginx buffering
                    'Content-Type': 'text/event-stream',
                    'Access-Control-Allow-Origin': '*'
                }
            )
        
        # Regular response
        elif isinstance(result, dict):
            if 'session_id' not in result:
                result['session_id'] = session_id
            return jsonify(result)
        
        else:
            return jsonify({'error': 'Unexpected response type', 'session_id': session_id}), 500
        
    except Exception as e:
        logger.error(f"ALADIN chat error: {e}")
        return jsonify({'error': str(e), 'session_id': session_id if 'session_id' in locals() else 'unknown'}), 500


# 3. ADD DEBUG ROUTE TO TEST INTERACTIVE AGENT:

@app.route('/debug/flask-context')
def debug_flask_context():
    """Debug route to test Flask context availability"""
    try:
        from flask import current_app
        
        context_info = {
            'has_current_app': hasattr(current_app, 'config'),
            'app_name': getattr(current_app, 'name', 'unknown'),
            'has_interactive_agent': hasattr(current_app, 'interactive_agent'),
            'has_data_agent': hasattr(current_app, 'data_agent') if hasattr(current_app, 'config') else False,
            'agents_available': []
        }
        
        # Check what agents are available
        if hasattr(current_app, 'interactive_agent') and current_app.interactive_agent:
            if hasattr(current_app.interactive_agent, 'orchestrator'):
                orchestrator = current_app.interactive_agent.orchestrator
                if hasattr(orchestrator, 'agents'):
                    context_info['agents_available'] = list(orchestrator.agents.keys())
        
        return jsonify({
            'success': True,
            'context_info': context_info,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        })


def enhance_response_with_agent_status(response, user_message):
    """
    COMPLETE REPLACEMENT: Enhance response with proper agent status information
    """
    # Initialize agent status based on message content
    agent_status = {
        'data': 'idle',
        'tools': 'idle', 
        'analysis': 'idle'
    }
    
    # Determine which agents should be active based on message
    message_lower = user_message.lower()
    
    if any(keyword in message_lower for keyword in ['analyze', 'data', 'price', 'volume']):
        agent_status['data'] = 'ready'
    
    if any(keyword in message_lower for keyword in ['chart', 'indicator', 'technical', 'rsi', 'macd', 'sma']):
        agent_status['tools'] = 'ready'
    
    if any(keyword in message_lower for keyword in ['analyze', 'analysis', 'compare', 'evaluate']):
        agent_status['analysis'] = 'ready'
    
    # Extract symbol if present for chart data
    detected_symbols = extract_symbols_from_message(user_message)
    
    # Build enhanced response
    enhanced_response = {
        'success': response.get('success', True),
        'response': clean_response_text(response.get('response', response.get('chat_response', ''))),
        'session_id': response.get('session_id'),
        'timestamp': response.get('timestamp', datetime.now().isoformat()),
        'agent_status': agent_status,
        'symbol': detected_symbols[0] if detected_symbols else None,
        'detected_symbols': detected_symbols
    }
    
    # Add chart data if symbol detected and analysis was successful
    if detected_symbols and response.get('success'):
        try:
            chart_data = get_chart_data_for_symbol(detected_symbols[0])
            if chart_data:
                enhanced_response['chart_data'] = chart_data
                agent_status['tools'] = 'ready'  # Tools agent processed successfully
        except Exception as e:
            logger.warning(f"Could not fetch chart data for {detected_symbols[0]}: {e}")
    
    return enhanced_response


def extract_symbols_from_message(message):
    """
    COMPLETE REPLACEMENT: Extract stock symbols from user message
    """
    import re
    
    # Common Indian stock symbols pattern
    symbol_patterns = [
        r'\b[A-Z]{2,10}\b',  # Basic pattern for stock symbols
        r'\b(BPCL|VOLTAMP|RELIANCE|TCS|INFY|HDFC|ICICI|SBI|ITC|LT)\b'  # Common symbols
    ]
    
    symbols = []
    for pattern in symbol_patterns:
        matches = re.findall(pattern, message.upper())
        symbols.extend(matches)
    
    # Remove duplicates and common English words
    excluded_words = {'THE', 'AND', 'OR', 'FOR', 'WITH', 'TO', 'IN', 'ON', 'AT', 'BY', 'FROM'}
    symbols = list(set([s for s in symbols if s not in excluded_words and len(s) >= 3]))
    
    return symbols[:3]  # Return max 3 symbols


def clean_response_text(text):
    """
    COMPLETE REPLACEMENT: Clean response text from debug content and emojis
    """
    if not text or not isinstance(text, str):
        return ''
    
    import re
    
    # Remove patterns we don't want in the UI
    patterns_to_remove = [
        r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+(AM|PM)',  # Timestamps
        r'\d{1,2}:\d{2}:\d{2}\s+(AM|PM)',  # Time only
        r'session[_\s]*id[:\s]*[\w\-]+',  # Session IDs
        r'ALADIN:\s*',  # ALADIN prefix
        r'You:\s*',  # You prefix
        r'[🎯🤖📊⚡🧙‍♂️🔮✨🎭🎪🎨🎬🎤🎧📈📉🔍💎⭐🌟💫🔥💪🚀]',  # Emojis
        r'behold[^.!?]*[.!?]',  # Mystical language
        r'o\s+seeker[^.!?]*[.!?]',  # Mystical content
        r'by\s+the\s+mystical[^.!?]*[.!?]',  # Mystical phrases
        r'weaving[^.!?]*[.!?]',  # Weaving references
        r'enchanted[^.!?]*[.!?]',  # Enchanted references
    ]
    
    cleaned = text
    for pattern in patterns_to_remove:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    # Clean up whitespace
    cleaned = ' '.join(cleaned.split())
    
    return cleaned.strip()


def get_chart_data_for_symbol(symbol):
    """
    COMPLETE REPLACEMENT: Get chart data for symbol from database
    """
    try:
        if not hasattr(app, 'data_agent') or not app.data_agent:
            return None
            
        # Use existing data agent to fetch chart data
        # This connects to your existing database structure
        query = """
            SELECT date, open, high, low, close, volume, indicators
            FROM market_data_enhanced 
            WHERE symbol = %s 
            ORDER BY date DESC 
            LIMIT 100
        """
        
        # Execute via data layer if available
        if hasattr(app.data_agent, 'data_layer'):
            with app.data_agent.data_layer.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (symbol,))
                    rows = cursor.fetchall()
                    
                    if rows:
                        return {
                            'price_data': [
                                {
                                    'date': row[0].isoformat() if row[0] else None,
                                    'open': float(row[1]) if row[1] else None,
                                    'high': float(row[2]) if row[2] else None,
                                    'low': float(row[3]) if row[3] else None,
                                    'close': float(row[4]) if row[4] else None,
                                    'volume': int(row[5]) if row[5] else None
                                }
                                for row in rows
                            ],
                            'indicators': row[6] if len(rows) > 0 and rows[0][6] else {}
                        }
        
        return None
        
    except Exception as e:
        logger.error(f"Error fetching chart data for {symbol}: {e}")
        return None


@app.route('/api/aladin/agent-status/<session_id>')
def get_agent_status(session_id):
    """
    NEW ENDPOINT: Get current agent status for session
    """
    try:
        if not hasattr(app, 'interactive_agent') or not app.interactive_agent:
            return jsonify({'error': 'Interactive agent not available'}), 503
        
        # Get workflow status if available
        status = {'data': 'ready', 'tools': 'ready', 'analysis': 'ready'}
        
        if hasattr(app.interactive_agent, 'get_workflow_status'):
            workflow_status = app.interactive_agent.get_workflow_status(session_id)
            if workflow_status.get('active_agents'):
                for agent in workflow_status['active_agents']:
                    if agent in status:
                        status[agent] = 'processing'
        
        return jsonify({
            'success': True,
            'agent_status': status,
            'session_id': session_id,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error getting agent status for {session_id}: {e}")
        return jsonify({'error': str(e)}), 500





@app.route('/api/aladin/chart/<symbol>')
def get_chart_data(symbol):
    """
    ENHANCED: Chart data with AI reasoning about timeframe selection
    
    NEW FEATURES:
    ✅ AI context about why this timeframe was chosen
    ✅ Technical indicator explanations
    ✅ Symbol analysis confidence scores
    """
    try:
        days = request.args.get('days', 30, type=int)
        session_id = request.args.get('session_id')  # Optional: link to analysis session
        
        # Get chart data using existing method
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database unavailable'}), 500
        
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT timestamp, open, high, low, close, volume, indicators
                FROM market_data_enhanced 
                WHERE symbol = %s 
                    AND timestamp >= NOW() - INTERVAL '%s days'
                ORDER BY timestamp ASC
            """, (symbol.upper(), days))
            
            rows = cursor.fetchall()
            
            if not rows:
                return jsonify({
                    'error': f'No data found for {symbol}',
                    'symbol': symbol
                }), 404
            
            # Format data for Chart.js
            chart_data = []
            latest_indicators = None
            
            for row in rows:
                timestamp, open_price, high_price, low_price, close_price, volume, indicators = row
                
                point = {
                    'x': timestamp.isoformat(),
                    'o': float(open_price),
                    'h': float(high_price), 
                    'l': float(low_price),
                    'c': float(close_price),
                    'v': int(volume)
                }
                
                # Add technical indicators if available
                if indicators:
                    point['indicators'] = indicators
                    latest_indicators = indicators  # Keep track of latest
                
                chart_data.append(point)
            
            # NEW: AI Context Generation
            ai_context = None
            if session_id and hasattr(app, 'interactive_agent') and app.interactive_agent:
                try:
                    if app.interactive_agent.transparency:
                        ai_context = f"📊 Chart Context: {days}-day view optimized for {symbol} analysis, capturing key price movements and technical signals"
                except:
                    pass
            
            # NEW: Technical Indicators Summary
            indicators_summary = None
            if latest_indicators:
                try:
                    indicators_summary = {
                        'rsi': latest_indicators.get('RSI_14'),
                        'macd': latest_indicators.get('MACD'),
                        'sma_20': latest_indicators.get('SMA_20'),
                        'bollinger_upper': latest_indicators.get('BB_upper'),
                        'bollinger_lower': latest_indicators.get('BB_lower')
                    }
                except:
                    indicators_summary = None
            
            return jsonify({
                'success': True,
                'symbol': symbol.upper(),
                'data': chart_data,
                'count': len(chart_data),
                'days_requested': days,
                'ai_context': ai_context,
                'indicators_summary': indicators_summary,
                'timeframe_reasoning': f"Selected {days}-day timeframe to capture recent trends and technical patterns for {symbol}",
                'timestamp': datetime.now().isoformat()
            })
        
    except Exception as e:
        logger.error(f"Chart data error for {symbol}: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if 'conn' in locals():
            conn.close()

@app.route('/api/aladin/news/<symbol>')
def get_news_events(symbol):
    """
    ✅ FIXED: News events endpoint with correct column names
    Returns news events with sentiment for chart markers
    """
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database unavailable'}), 500
        
        days = request.args.get('days', 30, type=int)
        
        with conn.cursor() as cursor:
            # FIXED: Use correct column names from news_insights table
            cursor.execute("""
                SELECT
                    id,
                    headline,
                    summary,
                    sentiment_score,
                    news_tier,
                    impact_magnitude,
                    published_at,
                    source_name,
                    source_url,
                    standardized_event_type,
                    affected_symbols
                FROM news_insights
                WHERE affected_symbols::jsonb ? %s
                    AND published_at >= NOW() - INTERVAL '%s days'
                    AND sentiment_score IS NOT NULL
                ORDER BY published_at DESC
                LIMIT 50
            """, (symbol.upper(), days))
            
            rows = cursor.fetchall()
            
            news_events = []
            for row in rows:
                (id, headline, summary, sentiment_score, news_tier, impact_magnitude,
                 published_at, source_name, source_url,
                 event_type, affected_symbols) = row
                
                news_events.append({
                    'id': id,
                    'headline': headline,
                    'summary': summary or '',
                    'sentiment_score': float(sentiment_score) if sentiment_score else 0,
                    'news_tier': news_tier,
                    'impact_magnitude': float(impact_magnitude) if impact_magnitude else 0,
                    'published_at': published_at.isoformat() if published_at else None,
                    'source_name': source_name,
                    'source_url': source_url,
                    'event_type': event_type,
                    'marker_color': get_sentiment_color(sentiment_score),
                    'affected_symbols': affected_symbols
                })
            
            return jsonify({
                'success': True,
                'symbol': symbol.upper(),
                'events': news_events,
                'count': len(news_events)
            })
        
    except Exception as e:
        logger.error(f"News data error for {symbol}: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if 'conn' in locals():
            conn.close()

def get_sentiment_color(sentiment_score):
    """Helper function to convert sentiment score to chart marker color"""
    if sentiment_score is None:
        return '#6b7280'  # Gray for unknown
    elif sentiment_score > 6:
        return '#22c55e'  # Green for positive (score > 6)
    elif sentiment_score >= 4:
        return '#ffc107'  # Yellow for neutral (score 4-6)
    else:
        return '#ef4444'  # Red for negative (score < 4)

@app.route('/api/aladin/quick/<symbol>', methods=['GET'])
def quick_symbol_data(symbol: str):
    """Quick symbol data for immediate display while full analysis runs"""
    try:
        # Get basic data from database
        days = request.args.get('days', 7, type=int)
        
        # Quick database query for chart data
        # This should connect to your existing database
        basic_data = {
            'symbol': symbol.upper(),
            'status': 'basic_data',
            'message': f'Basic data for {symbol.upper()}',
            'chart_available': True,
            'last_price': 'Loading...',
            'note': 'Detailed analysis in progress'
        }
        
        return jsonify(basic_data)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/aladin/static/<path:filename>')
def aladin_static(filename):
    """
    Serve ALADIN static files (CSS, JS)
    """
    try:
        # Try multiple possible paths
        static_paths = [
            '/opt/stockapp/aladin/static',
           
            'aladin/static'
        ]
        
        for static_path in static_paths:
            full_path = os.path.join(static_path, filename)
            if os.path.exists(full_path):
                return send_from_directory(static_path, filename)
        
        # If no file found, log the attempt
        logger.error(f"Static file not found: {filename}")
        logger.error(f"Searched paths: {static_paths}")
        
        # Return 404 with helpful message
        return f"Static file not found: {filename}", 404
        
    except Exception as e:
        logger.error(f"Static file error: {e}")
        return str(e), 500

# =====================================================
# NEW: AI Transparency endpoint
# =====================================================

@app.route('/api/aladin/transparency/<session_id>', methods=['GET'])
def get_ai_transparency_info(session_id):
    """
    NEW: Get AI transparency explanations for a session
    
    Returns all AI reasoning explanations for analytical decisions
    """
    try:
        if hasattr(app, 'interactive_agent') and app.interactive_agent:
            history = app.interactive_agent.get_conversation_history(session_id)
            
            # Filter for AI transparency messages
            transparency_messages = []
            for msg in history:
                if msg.get('role') == 'assistant':
                    message_text = msg.get('message', '')
                    if isinstance(message_text, dict):
                        message_text = message_text.get('response', '')
                    
                    # Look for transparency indicators
                    if any(indicator in message_text for indicator in ['🔮 Timeframe Logic:', '⚡ Indicator Strategy:', '🧞‍♂️ Analytical Approach:']):
                        transparency_type = 'unknown'
                        if '🔮 Timeframe Logic:' in message_text:
                            transparency_type = 'timeframe_reasoning'
                        elif '⚡ Indicator Strategy:' in message_text:
                            transparency_type = 'indicator_selection'
                        elif '🧞‍♂️ Analytical Approach:' in message_text:
                            transparency_type = 'methodology_explanation'
                        
                        transparency_messages.append({
                            'type': 'ai_transparency',
                            'transparency_type': transparency_type,
                            'message': message_text,
                            'timestamp': msg.get('timestamp')
                        })
            
            return jsonify({
                'success': True,
                'session_id': session_id,
                'transparency_explanations': transparency_messages,
                'count': len(transparency_messages),
                'timestamp': datetime.now().isoformat()
            })
        
        return jsonify({
            'success': False,
            'error': 'Session not found'
        }), 404
        
    except Exception as e:
        logger.error(f"AI transparency info error: {e}")
        return jsonify({'error': str(e)}), 500


# ============================================================================
# ENHANCED CORS FOR ALADIN (Modify existing after_request if present)
# ============================================================================

@app.after_request
def aladin_after_request(response):
    """
    Enhanced CORS headers for ALADIN frontend

    Note: If after_request already exists in app2.py, merge these headers
    """
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    response.headers.add('Access-Control-Max-Age', '86400')
    # Static images (logo, hero art) get served with Cache-Control: no-cache
    # by default — that forces a full re-download on every page load/refresh,
    # which is the actual cause of "slow" image rendering. These are static
    # files on disk that only change when we redeploy them, so let the
    # browser cache them.
    if request.path.startswith('/static/assets/'):
        response.headers['Cache-Control'] = 'public, max-age=604800, immutable'
        response.headers.pop('Pragma', None)
    return response

# ============================================================================
# FRONTEND COMPATIBILITY - ALL EXPECTED ENDPOINTS
# ============================================================================

# Kite legacy endpoints (frontend expects these)
@app.route('/api/kite/status', methods=['GET'])
@token_required
def kite_status(current_user):
    """Legacy Kite status endpoint - redirects to auth status"""
    return kite_auth_status(current_user)

@app.route('/api/kite/auth_url', methods=['GET'])
@token_required
def get_kite_auth_url(current_user):
    """Legacy Kite auth URL endpoint - redirects to auth login"""
    return kite_auth_login()

# CRITICAL FIX: Frontend expects this path without /api prefix
@app.route('/auth/kite/login', methods=['GET'])
def kite_auth_login_no_api():
    """Frontend expects this path for getKiteAuthUrl()"""
    return kite_auth_login()

# Breeze legacy endpoints (frontend expects these)  
@app.route('/api/breeze/status', methods=['GET'])
@token_required  
def breeze_status(current_user):
    """Legacy Breeze status endpoint - redirects to auth status"""
    return breeze_auth_status(current_user)

@app.route('/api/breeze/auth_url', methods=['GET'])
@token_required
def breeze_auth_url(current_user):
    """Legacy Breeze auth URL endpoint - redirects to auth login"""
    return breeze_auth_login()

# ============================================================================
# TRADING SCANNER API ENDPOINTS
# ============================================================================

@app.route('/api/scanner/candidates/fetch', methods=['POST'])
@token_required
def fetch_candidates(current_user):
    """
    Fetch candidates from NSE
    POST /api/scanner/candidates/fetch
    """
    try:
        from trading.components import CandidateFetcherService

        fetcher = CandidateFetcherService()
        count = fetcher.fetch_and_store_candidates()

        result = fetcher.get_candidates_for_strategy()

        return jsonify({
            'status': 'success',
            'message': f'Fetched {count} candidates',
            'data': result
        }), 200

    except Exception as e:
        logger.error(f"Candidate fetch failed: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/scanner/candidates/active', methods=['GET'])
@token_required
def get_active_candidates(current_user):
    """
    Get active candidates
    GET /api/scanner/candidates/active
    """
    try:
        from trading.components import CandidateFetcherService

        fetcher = CandidateFetcherService()
        result = fetcher.get_candidates_for_strategy()

        return jsonify({
            'status': 'success',
            'data': result
        }), 200

    except Exception as e:
        logger.error(f"Failed to get candidates: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/scanner/monitor/subscribe', methods=['POST'])
@token_required
def create_monitor_subscription(current_user):
    """
    Subscribe to monitoring service
    POST /api/scanner/monitor/subscribe

    Request body:
    {
        "strategy_name": "circuit_strategy",
        "symbols": ["INFY", "RELIANCE"],
        "monitor_frequency": 300,
        "required_fields": ["price", "volume", "timestamp"]
    }
    """
    try:
        from trading.components import MonitorService

        data = request.get_json()

        if not data or 'strategy_name' not in data or 'symbols' not in data:
            return jsonify({'error': 'Missing required fields: strategy_name, symbols'}), 400

        monitor = MonitorService()
        subscription_id = monitor.register_subscription(
            strategy_name=data['strategy_name'],
            symbols=data['symbols'],
            monitor_frequency=data.get('monitor_frequency', 300),
            required_fields=data.get('required_fields', ['price', 'volume', 'timestamp'])
        )

        if subscription_id:
            return jsonify({
                'status': 'success',
                'subscription_id': subscription_id,
                'message': f'Subscription created: {subscription_id}'
            }), 201
        else:
            return jsonify({'error': 'Failed to create subscription'}), 500

    except Exception as e:
        logger.error(f"Monitor subscription failed: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/scanner/monitor/poll', methods=['GET'])
@token_required
def poll_monitor_data(current_user):
    """
    Poll monitoring service for latest data
    GET /api/scanner/monitor/poll
    """
    try:
        from trading.components import MonitorService

        monitor = MonitorService()
        results = monitor.poll_subscriptions()

        return jsonify({
            'status': 'success',
            'timestamp': datetime.now().isoformat(),
            'data': results,
            'message': f'Polled {len(results)} strategies'
        }), 200

    except Exception as e:
        logger.error(f"Monitor poll failed: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/scanner/monitor/subscriptions', methods=['GET'])
@token_required
def get_monitor_subscriptions(current_user):
    """
    Get all active subscriptions
    GET /api/scanner/monitor/subscriptions
    """
    try:
        from trading.components.monitor_service import DatabaseHelper

        subscriptions = DatabaseHelper.get_active_subscriptions()

        return jsonify({
            'status': 'success',
            'count': len(subscriptions),
            'subscriptions': subscriptions
        }), 200

    except Exception as e:
        logger.error(f"Failed to get subscriptions: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/scanner/health', methods=['GET'])
def scanner_health():
    """
    Health check for scanner components
    GET /api/scanner/health
    """
    try:
        from trading.components.monitor_service import DatabaseHelper
        from trading.components import CandidateFetcherService

        # Check database connectivity
        conn = DatabaseHelper.get_connection()
        db_status = 'ok' if conn else 'error'
        if conn:
            conn.close()

        # Get active candidates and subscriptions
        candidates = CandidateFetcherService().get_candidates_for_strategy()
        subscriptions = DatabaseHelper.get_active_subscriptions()

        return jsonify({
            'status': 'healthy' if db_status == 'ok' else 'degraded',
            'database': db_status,
            'components': {
                'nse_fetcher': 'ok',
                'monitor_service': 'ok',
                'breeze_api': 'configured'
            },
            'metrics': {
                'active_candidates': candidates['count'],
                'active_subscriptions': len(subscriptions)
            },
            'timestamp': datetime.now().isoformat()
        }), 200

    except Exception as e:
        logger.error(f"Scanner health check failed: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

# ============================================================================
# SSL CERTIFICATE FINDER - PRESERVED
# ============================================================================

def find_ssl_certificates():
    """Find SSL certificates for HTTPS"""
    cert_paths = [
        (os.path.join(os.getcwd(), 'cert.pem'), os.path.join(os.getcwd(), 'key.pem')),
        ('/opt/stockapp/stockmart/cert.pem', '/opt/stockapp/stockmart/key.pem'),
        ('/etc/ssl/certs/stockmart-cert.pem', '/etc/ssl/private/stockmart-key.pem')
    ]
    
    for cert_file, key_file in cert_paths:
        if os.path.exists(cert_file) and os.path.exists(key_file):
            try:
                with open(cert_file, 'r'), open(key_file, 'r'):
                    logger.info(f"Using SSL certificates: {cert_file}, {key_file}")
                    return (cert_file, key_file)
            except Exception as e:
                logger.warning(f"Error reading certificates: {e}")
    
    logger.warning("No valid SSL certificates found")
    return None

# ============================================================================
# APPLICATION STARTUP - SIMPLIFIED
# ============================================================================

# Homepage route - Add this to app2.py before the main section

# Homepage route - Fixed CSS and complete styling
# Replace the existing homepage route in app2.py with this updated version:

# Add this to your app2.py
@app.route('/react')
def react_app():
    return send_from_directory('/opt/stockapp/aladin/react/dist', 'index.html')


@app.route('/')
def homepage():
    """Smart homepage that redirects to ALADIN for the main domain"""
    
    # Check if accessed via alaidin.info domain
    host = request.headers.get('Host', '').lower()
    
    if 'alaidin.info' in host:
        # Redirect to the dashboard for the main domain — /dashboard sends
        # unauthenticated visitors on to /subscribe itself, so this covers
        # both "logged-in users land on their dashboard" and "new visitors
        # see subscribe, not chat" without a second branch here.
        return redirect('/dashboard')
    
    # Show API homepage for IP access or other domains
    return """<!DOCTYPE html>
<html>
<head>
    <title>StockMart API Server</title>
    <style>
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0; padding: 20px; background-color: #f5f5f5; color: #333;
        }
        .header { color: #2563eb; margin-bottom: 10px; }
        .status { color: #16a34a; font-weight: 500; }
        .endpoint { 
            background: white; padding: 15px; margin: 10px 0; 
            border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .endpoint a { color: #2563eb; text-decoration: none; }
        .endpoint a:hover { text-decoration: underline; }
        .aladin-link {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white !important;
            font-weight: bold;
            font-size: 1.1em;
        }
    </style>
</head>
<body>
    <h1 class="header">🚀 StockMart API Server</h1>
    <p class="status">✅ Service running successfully</p>
    
    <h2>Available Endpoints:</h2>
    
    <div class="endpoint aladin-link">
        <strong>🎯 ALADIN Dashboard (Main App):</strong><br>
        <a href="/aladin" target="_blank">Access ALADIN →</a>
    </div>
    
    <div class="endpoint">
        <strong>Health Check:</strong><br>
        <a href="/api/health" target="_blank">/api/health</a>
    </div>
    
    <div class="endpoint">
        <strong>Authentication:</strong><br>
        <a href="/api/auth/kite/login" target="_blank">/api/auth/kite/login</a>
    </div>
    
    <p>🔒 HTTPS Enabled | 🔄 Auto-restart | 🌐 Background Service</p>
    <p>🌟 <strong>Main App:</strong> <a href="/aladin">ALADIN Trading Dashboard</a></p>
</body>
</html>
"""

# ============================================================================
# Subscriber Dashboard routes
# ============================================================================

def _show_master_preview(user_id, role) -> bool:
    """True for observers, and for subscribers who haven't invested in anything yet —
    both should see a ₹1L/trade-normalized preview of the master account's real trades
    instead of their own (empty, or nonexistent) history."""
    if role == 'observer':
        return True
    if role == 'subscriber':
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import SubscriberDB
        subs = SubscriberDB.get_user_subscriptions(user_id)
        return not any(s.get('enabled') for s in subs)
    return False


def _live_strategy_ids(mode='live'):
    """Strategy IDs currently in the given mode ('live' | 'shadow' | 'all'),
    read from the same live strategy_controls + STRATEGY_CONFIG source
    observe_strategies() classifies from — NOT a hardcoded list. A hardcoded
    tuple (the old _LIVE_SIDS pattern) silently goes stale the moment a new
    strategy goes live (this is exactly how PVI_EOD went missing from every
    "combined" P&L view after it shipped live on Jul 3)."""
    sys.path.insert(0, '/opt/stockapp/tradingv2')
    from db.queries import StrategyControlsDB
    from config import STRATEGY_CONFIG
    controls = {c['strategy_id']: c for c in StrategyControlsDB.get_all()}
    result = []
    for sid, cfg in STRATEGY_CONFIG.items():
        if not cfg.get('enabled', False):
            continue
        ctrl = controls.get(sid, {})
        live = ctrl.get('live_enabled', cfg.get('live_enabled', False))
        strat_mode = 'live' if live else 'shadow'
        if mode == 'all' or strat_mode == mode:
            result.append(sid)
    return result


def _sub_session_user():
    """Return (user_id, name) from Flask session, or (None, None) if not logged in."""
    uid = session.get('sub_user_id')
    return (uid, session.get('sub_name', '')) if uid else (None, None)


@app.route('/dashboard', methods=['GET'])
def subscriber_dashboard():
    """Personal console — owner, subscriber, or observer session required."""
    user_id, name = _sub_session_user()
    logger.info(
        f"[dashboard] session_cookie={'YES' if request.cookies.get('session') else 'NO'} "
        f"sub_user_id={session.get('sub_user_id')!r} "
        f"sub_role={session.get('sub_role')!r} "
        f"session_keys={list(session.keys())}"
    )
    if not user_id:
        return redirect('/subscribe')
    page_path = '/opt/stockapp/aladin/static/dashboard.html'
    try:
        with open(page_path, 'r') as f:
            html = f.read()
    except FileNotFoundError:
        return "Dashboard not found", 404
    role = session.get('sub_role', 'subscriber')
    html = html.replace('__USER_ROLE__', role)
    return html, 200, {'Content-Type': 'text/html', 'Cache-Control': 'no-store, no-cache, must-revalidate'}


@app.route('/api/subscriber/me', methods=['GET'])
def subscriber_me():
    """Current user profile + strategy subscriptions."""
    user_id, name = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    role = session.get('sub_role', 'subscriber')
    if role == 'observer':
        return jsonify({
            'user_id':       user_id,
            'name':          name,
            'role':          role,
            'username':      session.get('sub_kite_id', name),
            'subscriptions': [],
        })
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import SubscriberDB, _exec
        profile = SubscriberDB.get_subscriber_profile(user_id)
        if not profile:
            return jsonify({'error': 'user_not_found'}), 404
        pending_approval, is_active = False, True
        if role == 'subscriber':
            bs = _exec(
                "SELECT pending_approval, is_active FROM user_broker_sessions "
                "WHERE user_id = %s ORDER BY created_at DESC LIMIT 1",
                (user_id,), fetch='one',
            )
            pending_approval = bool(bs and bs.get('pending_approval'))
            is_active = bool(bs and bs.get('is_active'))
        return jsonify({
            'user_id':          user_id,
            'name':             name,
            'role':             role,
            'username':         profile.get('username', ''),
            'subscriptions':    profile.get('subscriptions') or [],
            'pending_approval': pending_approval,
            'is_active':        is_active,
        })
    except Exception as e:
        logger.error(f"[subscriber/me] {e}")
        return jsonify({'error': 'server_error'}), 500


_subscriber_pnl_cache: dict = {}   # (user_id, role, period, strategy_id, mode) -> (monotonic_ts, payload_dict)
_SUBSCRIBER_PNL_TTL_S = 20.0        # just under the 30s dashboard poll interval

@app.route('/api/subscriber/pnl', methods=['GET'])
def subscriber_pnl():
    """P&L summary by strategy, gross and net (after charges). Includes both open and closed trades in counts."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401

    period      = request.args.get('period', 'all')     # today | fy | all
    strategy_id = request.args.get('strategy') or None
    mode        = request.args.get('mode', 'live')       # live | shadow | all — ignored when strategy_id is set
    role        = session.get('sub_role', 'subscriber')

    # 30s dashboard auto-poll (dashboard.html refreshAll, every open tab) hits this
    # endpoint unconditionally regardless of period — "all"/"fy" recompute full
    # trade history + per-trade charges every single cycle, forever, for as long
    # as a tab is open. Short per-worker TTL cache stops that repeated recompute
    # from stacking across concurrent tabs/subscribers within the same window.
    _pnl_cache_key = (user_id, role, period, strategy_id, mode)
    _pnl_cached = _subscriber_pnl_cache.get(_pnl_cache_key)
    if _pnl_cached and (time.monotonic() - _pnl_cached[0]) < _SUBSCRIBER_PNL_TTL_S:
        return jsonify(_pnl_cached[1])

    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import SubscriberDB
        from trading_costs import compute_charges

        show_master_preview = _show_master_preview(user_id, role)

        # ── Owner: query trades_v2 (master account), real numbers ───────────
        if role == 'owner':
            from db.queries import _exec
            from datetime import date as _dt_date
            fy_year  = _dt_date.today().year if _dt_date.today().month >= 4 else _dt_date.today().year - 1
            fy_start = _dt_date(fy_year, 4, 1)

            # Strategies in the requested mode, read live — not a hardcoded list
            _LIVE_SIDS = _live_strategy_ids(mode)

            period_clause_closed = ''
            period_clause_open   = ''
            period_params: list  = []
            if period == 'today':
                # Use exit_time so overnight-hold strategies (e.g. EODR) whose
                # trade_date is the entry day still appear when they exit today.
                period_clause_closed = (
                    "AND exit_time >= DATE_TRUNC('day', NOW() AT TIME ZONE 'Asia/Kolkata')"
                    " AT TIME ZONE 'Asia/Kolkata'"
                )
                # No entry_time floor here — an open position is "today's open
                # count" regardless of which day it was entered (same bug class
                # as subscriber_live/kite-pnl/get_all_strategies_pnl* fixed
                # earlier this session: an entry_time>=today floor silently
                # dropped multi-day-carried positions, e.g. weekend-carried
                # PVI_EOD, from n_open/the "Trades" tile's live count).
                period_clause_open = ''
            elif period == 'fy':
                period_clause_closed = "AND trade_date >= %s"
                period_clause_open   = "AND trade_date >= %s"
                period_params = [fy_start]

            if strategy_id:
                sid_clause = 'AND strategy_id = %s'
                sid_params = [strategy_id]
            elif _LIVE_SIDS:
                placeholders = ','.join(['%s'] * len(_LIVE_SIDS))
                sid_clause = f'AND strategy_id IN ({placeholders})'
                sid_params = list(_LIVE_SIDS)
            else:
                sid_clause = 'AND FALSE'  # no strategies in this mode right now
                sid_params = []

            # trades_v2 has pnl_rupees (gross) and capital_deployed — no direction/qty
            closed_rows = _exec(
                f"""SELECT strategy_id, product,
                           CAST(pnl_rupees       AS FLOAT) AS pnl_rupees,
                           CAST(capital_deployed AS FLOAT) AS capital_deployed,
                           (entry_time AT TIME ZONE 'Asia/Kolkata')::date
                               != (exit_time AT TIME ZONE 'Asia/Kolkata')::date AS held_overnight
                    FROM trades_v2
                    WHERE exit_time IS NOT NULL {period_clause_closed} {sid_clause}""",
                tuple(period_params + sid_params), fetch='all'
            ) or []

            open_rows = _exec(
                f"""SELECT strategy_id FROM trades_v2
                    WHERE exit_time IS NULL {period_clause_open} {sid_clause}""",
                tuple(period_params + sid_params), fetch='all'
            ) or []

            from trading_costs import charges_for_trade as _cft
            by_strategy: dict = {}
            for t in closed_rows:
                gross   = float(t['pnl_rupees'] or 0)
                cap     = float(t['capital_deployed'] or 0)
                sid     = t['strategy_id']
                charges = _cft(cap, sid, product=t.get('product'),
                                held_overnight=t.get('held_overnight', True)) if cap > 0 else 0.0
                net     = gross - charges
                if sid not in by_strategy:
                    by_strategy[sid] = {'strategy_id': sid, 'n_closed': 0, 'n_open': 0,
                                        'n_wins': 0, 'gross_pnl_rs': 0.0, 'charges_rs': 0.0,
                                        'net_pnl_rs': 0.0, 'capital_per_trade': 0}
                by_strategy[sid]['n_closed']     += 1
                by_strategy[sid]['n_wins']       += 1 if gross > 0 else 0
                by_strategy[sid]['gross_pnl_rs']  = round(by_strategy[sid]['gross_pnl_rs'] + gross, 2)
                by_strategy[sid]['charges_rs']    = round(by_strategy[sid]['charges_rs'] + charges, 2)
                by_strategy[sid]['net_pnl_rs']    = round(by_strategy[sid]['net_pnl_rs'] + net, 2)

            for t in open_rows:
                sid = t['strategy_id']
                if sid not in by_strategy:
                    by_strategy[sid] = {'strategy_id': sid, 'n_closed': 0, 'n_open': 0,
                                        'n_wins': 0, 'gross_pnl_rs': 0.0, 'charges_rs': 0.0,
                                        'net_pnl_rs': 0.0, 'capital_per_trade': 0}
                by_strategy[sid]['n_open'] += 1

            for sid in by_strategy:
                by_strategy[sid]['n_trades'] = by_strategy[sid]['n_closed'] + by_strategy[sid]['n_open']

            base_capital_map = {'BLOCK_DEAL_FADE': 200_000, 'PVI': 200_000, 'HA_REVERSAL': 200_000, 'ZONE_S21': 150_000}
            result = list(by_strategy.values())
            for r in result:
                r['base_capital'] = base_capital_map.get(r['strategy_id'], 200_000)

            # Peak daily capital. Portfolio-wide "today" uses the Kite-margins()-
            # derived tracker (DailyCapitalDB, migration 016) — accurate because
            # it reads Kite's own capital-in-use figure directly instead of
            # summing trades_v2.capital_deployed, which double-counts capital
            # reused across same-day trades (2026-07-20: ~35 trades reusing the
            # same ~4-500k pool looked like millions under the old SUM) and drops
            # capital still tied up in positions carried from a prior day. Only
            # available from 2026-07-20 onward and only portfolio-wide (margins()
            # is account-level, not per-strategy) — fall back to the old
            # SUM-by-trade_date approximation for a single-strategy filter, for
            # FY/all-time periods reaching back before tracking started, OR when
            # today's snapshot row doesn't exist yet / has no peak recorded (the
            # tracker needs a pre-market capture + at least one orchestrator
            # cycle to have run under the new code — until then this falls
            # through to the old figure rather than showing a bare 0).
            _peak = 0.0
            if period == 'today' and not strategy_id:
                from db.queries import DailyCapitalDB
                _snap = DailyCapitalDB.get(_dt_date.today())
                _peak = float((_snap or {}).get('peak_capital_used') or 0)
            if not (period == 'today' and not strategy_id and _peak > 0):
                if period == 'today':
                    _pk_clause = (
                        "AND entry_time >= DATE_TRUNC('day', NOW() AT TIME ZONE 'Asia/Kolkata')"
                        " AT TIME ZONE 'Asia/Kolkata'"
                    )
                    _pk_params = list(sid_params)
                elif period == 'fy':
                    _pk_clause  = "AND trade_date >= %s::date"
                    _pk_params  = [fy_start] + list(sid_params)
                else:
                    _pk_clause  = ''
                    _pk_params  = list(sid_params)
                _daily = _exec(
                    f"""SELECT trade_date,
                               SUM(CAST(capital_deployed AS FLOAT)) AS daily_cap
                        FROM trades_v2
                        WHERE 1=1 {_pk_clause} {sid_clause}
                        GROUP BY trade_date""",
                    tuple(_pk_params), fetch='all'
                ) or []
                _peak = max((float(r.get('daily_cap') or 0) for r in _daily), default=0.0)

            totals = {
                'strategy_id':         '__total__',
                'n_trades':            sum(r['n_trades']    for r in result),
                'n_closed':            sum(r['n_closed']    for r in result),
                'n_open':              sum(r['n_open']      for r in result),
                'n_wins':              sum(r['n_wins']      for r in result),
                'gross_pnl_rs':        round(sum(r['gross_pnl_rs'] for r in result), 2),
                'charges_rs':          round(sum(r['charges_rs']   for r in result), 2),
                'net_pnl_rs':          round(sum(r['net_pnl_rs']   for r in result), 2),
                'capital_per_trade':   0,
                'peak_daily_capital':  round(_peak, 0),
            }
            _pnl_payload = {'rows': result, 'totals': totals, 'period': period, 'base_capital_map': base_capital_map}
            _subscriber_pnl_cache[_pnl_cache_key] = (time.monotonic(), _pnl_payload)
            return jsonify(_pnl_payload)

        # ── Observer / not-yet-invested subscriber: master trades_v2, scaled up
        #    per-strategy by 1.0/capital_scale (the live sizing dial) so the
        #    viewer sees what today would look like at dial=1.0 instead of its
        #    current live value — NOT forced onto a flat ₹1L/trade basis. That
        #    flat-basis approach (blended ratio applied once, or per-trade
        #    normalization) was tried and rejected on Jul 13 2026: per-trade
        #    normalization let one oddly-sized trade flip the sign of the day
        #    (PVI: raw +₹292 reported as -₹329); the blended-ratio fix that
        #    replaced it then diluted a real +₹1,506 PVI_EOD day down to +₹624,
        #    which read as worse than reality. Scaling every trade by the same
        #    strategy-constant factor (1/capital_scale) has neither problem —
        #    it preserves the real relative sizing between trades (rank,
        #    regime, add-ons) exactly, just replayed at a bigger dial setting.
        #    See queries.py get_all_strategies_pnl_normalized() for the SQL
        #    equivalent used elsewhere.
        elif show_master_preview:
            from db.queries import _exec
            from trading_costs import charges_for_trade as _cft
            from datetime import date as _dt_date
            fy_year  = _dt_date.today().year if _dt_date.today().month >= 4 else _dt_date.today().year - 1
            fy_start = _dt_date(fy_year, 4, 1)

            _scale_rows = _exec("SELECT strategy_id, capital_scale FROM strategy_controls", fetch='all') or []
            _scale_factor = {
                r['strategy_id']: (1.0 / float(r['capital_scale']))
                for r in _scale_rows if r['capital_scale'] and float(r['capital_scale']) > 0
            }

            _LIVE_SIDS = _live_strategy_ids(mode)
            period_clause_closed = ''
            period_clause_open   = ''
            period_params: list  = []
            if period == 'today':
                period_clause_closed = (
                    "AND exit_time >= DATE_TRUNC('day', NOW() AT TIME ZONE 'Asia/Kolkata')"
                    " AT TIME ZONE 'Asia/Kolkata'"
                )
                # No entry_time floor — see comment on the owner branch above.
                period_clause_open = ''
            elif period == 'fy':
                period_clause_closed = "AND trade_date >= %s"
                period_clause_open   = "AND trade_date >= %s"
                period_params = [fy_start]

            if strategy_id:
                sid_clause = 'AND strategy_id = %s'
                sid_params = [strategy_id]
            elif _LIVE_SIDS:
                placeholders = ','.join(['%s'] * len(_LIVE_SIDS))
                sid_clause = f'AND strategy_id IN ({placeholders})'
                sid_params = list(_LIVE_SIDS)
            else:
                sid_clause = 'AND FALSE'
                sid_params = []

            closed_rows = _exec(
                f"""SELECT strategy_id, product,
                           CAST(pnl_rupees       AS FLOAT) AS pnl_rupees,
                           CAST(capital_deployed AS FLOAT) AS capital_deployed,
                           (entry_time AT TIME ZONE 'Asia/Kolkata')::date
                               != (exit_time AT TIME ZONE 'Asia/Kolkata')::date AS held_overnight
                    FROM trades_v2
                    WHERE exit_time IS NOT NULL {period_clause_closed} {sid_clause}""",
                tuple(period_params + sid_params), fetch='all'
            ) or []
            open_rows = _exec(
                f"""SELECT strategy_id FROM trades_v2
                    WHERE exit_time IS NULL {period_clause_open} {sid_clause}""",
                tuple(period_params + sid_params), fetch='all'
            ) or []

            # Scale each real trade by its strategy's constant factor
            # (1/capital_scale) — safe per-trade because the factor doesn't
            # vary by trade size, unlike the old flat-basis normalization.
            by_strategy: dict = {}
            for t in closed_rows:
                cap          = float(t['capital_deployed'] or 0)
                gross_actual = float(t['pnl_rupees'] or 0)
                sid          = t['strategy_id']
                factor       = _scale_factor.get(sid, 1.0)
                cap_scaled   = cap * factor
                gross_scaled = gross_actual * factor
                product      = t.get('product')
                if sid not in by_strategy:
                    by_strategy[sid] = {'strategy_id': sid, 'n_closed': 0, 'n_open': 0,
                                        'n_wins': 0, 'gross_pnl_rs': 0.0, 'charges_rs': 0.0,
                                        'net_pnl_rs': 0.0, 'sum_capital_scaled': 0.0}
                r = by_strategy[sid]
                charges = _cft(cap_scaled, sid, product=product,
                                held_overnight=t.get('held_overnight', True)) if cap_scaled else 0.0
                r['n_closed']            += 1
                r['n_wins']              += 1 if gross_actual > 0 else 0
                r['gross_pnl_rs']         = round(r['gross_pnl_rs'] + gross_scaled, 2)
                r['charges_rs']           = round(r['charges_rs'] + charges, 2)
                r['net_pnl_rs']           = round(r['net_pnl_rs'] + gross_scaled - charges, 2)
                r['sum_capital_scaled']  += cap_scaled

            for t in open_rows:
                sid = t['strategy_id']
                if sid not in by_strategy:
                    by_strategy[sid] = {'strategy_id': sid, 'n_closed': 0, 'n_open': 0,
                                        'n_wins': 0, 'gross_pnl_rs': 0.0, 'charges_rs': 0.0,
                                        'net_pnl_rs': 0.0, 'sum_capital_scaled': 0.0}
                by_strategy[sid]['n_open'] += 1

            for sid, r in by_strategy.items():
                r['n_trades'] = r['n_closed'] + r['n_open']
                r['capital_per_trade'] = round(r['sum_capital_scaled'] / r['n_closed'], 2) if r['n_closed'] else 0.0
                del r['sum_capital_scaled']

            base_capital_map = {'BLOCK_DEAL_FADE': 200_000, 'PVI': 200_000, 'HA_REVERSAL': 200_000, 'ZONE_S21': 150_000}
            result = list(by_strategy.values())
            for r in result:
                r['base_capital'] = base_capital_map.get(r['strategy_id'], 200_000)

            totals = {
                'strategy_id':       '__total__',
                'n_trades':          sum(r['n_trades']    for r in result),
                'n_closed':          sum(r['n_closed']    for r in result),
                'n_open':            sum(r['n_open']      for r in result),
                'n_wins':            sum(r['n_wins']      for r in result),
                'gross_pnl_rs':      round(sum(r['gross_pnl_rs'] for r in result), 2),
                'charges_rs':        round(sum(r['charges_rs']   for r in result), 2),
                'net_pnl_rs':        round(sum(r['net_pnl_rs']   for r in result), 2),
            }
            _pnl_payload = {'rows': result, 'totals': totals, 'period': period,
                            'base_capital_map': base_capital_map, 'normalized': True,
                            'scale_factors': _scale_factor}
            _subscriber_pnl_cache[_pnl_cache_key] = (time.monotonic(), _pnl_payload)
            return jsonify(_pnl_payload)

        # ── Subscriber: query subscriber_trades ───────────────────────────────
        closed_trades, open_trades = SubscriberDB.get_subscriber_trades_for_pnl(user_id, strategy_id, period)

        by_strategy: dict = {}

        # Process CLOSED trades for P&L calculation
        for t in closed_trades:
            ep  = float(t['entry_price'])
            xp  = float(t['exit_price'])
            qty = float(t['qty'])
            cap = ep * qty
            gross = (xp - ep) * qty if t['direction'] == 'LONG' else (ep - xp) * qty
            charges = compute_charges(cap)['total'] if cap > 0 else 0.0
            net = gross - charges

            sid = t['strategy_id']
            if sid not in by_strategy:
                by_strategy[sid] = {
                    'strategy_id': sid, 'n_closed': 0, 'n_open': 0, 'n_wins': 0,
                    'gross_pnl_rs': 0.0, 'charges_rs': 0.0, 'net_pnl_rs': 0.0,
                    'capital_per_trade': t.get('capital_per_trade') or 0,
                }
            by_strategy[sid]['n_closed']    += 1
            by_strategy[sid]['n_wins']      += 1 if gross > 0 else 0
            by_strategy[sid]['gross_pnl_rs'] = round(by_strategy[sid]['gross_pnl_rs'] + gross, 2)
            by_strategy[sid]['charges_rs']   = round(by_strategy[sid]['charges_rs'] + charges, 2)
            by_strategy[sid]['net_pnl_rs']   = round(by_strategy[sid]['net_pnl_rs'] + net, 2)

        # Process OPEN trades for count only (no P&L)
        for t in open_trades:
            sid = t['strategy_id']
            if sid not in by_strategy:
                by_strategy[sid] = {
                    'strategy_id': sid, 'n_closed': 0, 'n_open': 0, 'n_wins': 0,
                    'gross_pnl_rs': 0.0, 'charges_rs': 0.0, 'net_pnl_rs': 0.0,
                    'capital_per_trade': t.get('capital_per_trade') or 0,
                }
            by_strategy[sid]['n_open'] += 1

        # Add total trades count (closed + open)
        for sid in by_strategy:
            by_strategy[sid]['n_trades'] = by_strategy[sid]['n_closed'] + by_strategy[sid]['n_open']

        # Get base capital map for scaling
        base_capital_map = {
            'BLOCK_DEAL_FADE': 200_000,
            'PVI': 200_000,
            'HA_REVERSAL': 200_000,
            'ZONE_S21': 150_000,
        }

        result = list(by_strategy.values())

        # Add base_capital to each strategy row for frontend scaling
        for r in result:
            r['base_capital'] = base_capital_map.get(r['strategy_id'], 200_000)
        # Totals across all strategies
        totals = {
            'strategy_id':   '__total__',
            'n_trades':      sum(r['n_trades'] for r in result),
            'n_closed':      sum(r['n_closed'] for r in result),
            'n_open':        sum(r['n_open'] for r in result),
            'n_wins':        sum(r['n_wins'] for r in result),
            'gross_pnl_rs':  round(sum(r['gross_pnl_rs'] for r in result), 2),
            'charges_rs':    round(sum(r['charges_rs'] for r in result), 2),
            'net_pnl_rs':    round(sum(r['net_pnl_rs'] for r in result), 2),
            'capital_per_trade': 0,
        }
        _pnl_payload = {'rows': result, 'totals': totals, 'period': period, 'base_capital_map': base_capital_map}
        _subscriber_pnl_cache[_pnl_cache_key] = (time.monotonic(), _pnl_payload)
        return jsonify(_pnl_payload)
    except Exception as e:
        logger.error(f"[subscriber/pnl] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/subscriber/trades', methods=['GET'])
def subscriber_trades():
    """Paginated trade history."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401

    strategy_id = request.args.get('strategy') or None
    status      = request.args.get('status') or None
    period      = request.args.get('period') or None
    page        = max(1, int(request.args.get('page', 1)))
    limit       = 50
    offset      = (page - 1) * limit
    role        = session.get('sub_role', 'subscriber')

    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from trading_costs import compute_charges

        if role in ('owner', 'observer'):
            from db.queries import _exec
            sid_clause    = 'AND strategy_id = %s' if strategy_id else ''
            status_clause = "AND exit_time IS NULL" if status == 'open' else ("AND exit_time IS NOT NULL" if status == 'closed' else '')
            _today_start  = "DATE_TRUNC('day', NOW() AT TIME ZONE 'Asia/Kolkata') AT TIME ZONE 'Asia/Kolkata'"
            if period == 'today':
                if status == 'closed':
                    period_clause = f"AND exit_time  >= {_today_start}"
                else:
                    period_clause = f"AND entry_time >= {_today_start}"
            else:
                period_clause = ''
            params = ([strategy_id] if strategy_id else []) + [limit + 1, offset]
            rows = _exec(
                f"""SELECT trade_id::text AS id, strategy_id, symbol,
                           CASE strategy_id
                             WHEN 'EODR'              THEN 'LONG'
                             WHEN 'ORB_SHORT'         THEN 'SHORT'
                             WHEN 'BLOCK_DEAL_FADE'   THEN 'LONG'
                             WHEN 'BLOCK_DEAL_BOUNCE' THEN 'LONG'
                             WHEN 'PVI'               THEN 'SHORT'
                             WHEN 'PVI_EOD'           THEN 'LONG'
                             WHEN 'FRS'               THEN 'SHORT'
                             ELSE NULL
                           END AS direction,
                           strategy_id IN ('EODR', 'PVI_EOD') AS is_overnight,
                           product,
                           CAST(entry_price  AS FLOAT) AS entry_price,
                           CAST(exit_price   AS FLOAT) AS exit_price,
                           CAST(stop_price   AS FLOAT) AS stop_price,
                           CAST(target_price AS FLOAT) AS target_price,
                           CAST(pnl_pct      AS FLOAT) AS pnl_pct,
                           NULL::FLOAT AS qty,
                           CAST(capital_deployed AS FLOAT) AS capital_deployed,
                           CAST(pnl_rupees    AS FLOAT) AS gross_pnl_rs,
                           CAST(capital_deployed AS FLOAT) AS _cap,
                           entry_time, exit_time, exit_type, trade_date::text AS trade_date,
                           CASE WHEN exit_time IS NOT NULL THEN 'closed' ELSE 'open' END AS status,
                           agent_approved_by,
                           metadata::text AS metadata
                    FROM trades_v2
                    WHERE 1=1 {sid_clause} {status_clause} {period_clause}
                    ORDER BY COALESCE(exit_time, entry_time) DESC
                    LIMIT %s OFFSET %s""",
                tuple(params), fetch='all'
            ) or []
        else:
            from db.queries import SubscriberDB
            rows = SubscriberDB.get_subscriber_all_trades(user_id, strategy_id, status, limit, offset)

        has_more = len(rows) > limit
        if has_more:
            rows = rows[:limit]

        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from trading_costs import (charges_for_trade as _cft, compute_charges as _cc,
                                    _cnc_strategies as _cnc_fn, held_overnight as _ho)
        from exit_taxonomy import classify_exit
        _CNC = _cnc_fn()
        for r in rows:
            # Owner rows have gross_pnl_rs pre-filled from pnl_rupees
            cap = 0.0
            if r.get('gross_pnl_rs') is not None:
                cap  = float(r.get('_cap') or r.get('capital_deployed') or 0)
                _pt  = (r.get('product') or '').upper() or ('CNC' if r.get('strategy_id', '').upper() in _CNC else 'MIS')
                ho   = _ho(r.get('entry_time'), r.get('exit_time'))
                chg  = _cft(cap, r.get('strategy_id', ''), product=_pt, held_overnight=ho) if cap > 0 else 0.0
                r['charges_rs']        = round(chg, 2)
                r['net_pnl_rs']        = round(float(r['gross_pnl_rs']) - chg, 2)
                r['charges_breakdown'] = _cc(cap, _pt, held_overnight=ho) if cap > 0 else None
            else:
                ep  = r.get('entry_price') or 0
                xp  = r.get('exit_price')
                qty = r.get('qty') or 0
                if ep and xp and qty:
                    cap   = ep * qty
                    gross = (float(xp) - ep) * qty if r.get('direction') == 'LONG' else (ep - float(xp)) * qty
                    _pt   = (r.get('product') or '').upper() or ('CNC' if r.get('strategy_id', '').upper() in _CNC else 'MIS')
                    ho    = _ho(r.get('entry_time'), r.get('exit_time'))
                    chg   = _cft(cap, r.get('strategy_id', ''), product=_pt, held_overnight=ho) if cap > 0 else 0.0
                    r['gross_pnl_rs']      = round(gross, 2)
                    r['charges_rs']        = round(chg, 2)
                    r['net_pnl_rs']        = round(gross - chg, 2)
                    r['charges_breakdown'] = _cc(cap, _pt, held_overnight=ho) if cap > 0 else None
                else:
                    r['gross_pnl_rs'] = r['charges_rs'] = r['net_pnl_rs'] = None
                    r['charges_breakdown'] = None
                    cap = 0.0
            r.pop('_cap', None)

            # pnl_pct: owner rows already have it from the trades_v2 column; subscriber
            # rows (computed above from entry/exit/qty) don't — derive it here too.
            if r.get('pnl_pct') is None and cap and r.get('gross_pnl_rs') is not None:
                r['pnl_pct'] = round(float(r['gross_pnl_rs']) / cap * 100, 2)

            # Parse metadata JSON string (owner path returns text)
            if isinstance(r.get('metadata'), str):
                try:
                    import json as _json
                    r['metadata'] = _json.loads(r['metadata'])
                except Exception:
                    r['metadata'] = None

            # Unified exit taxonomy: bucket the raw exit_type into T1/T0/S0/S1/MANUAL/EOD
            # (or None for NO_FILL) using realized pnl to resolve PnL-ambiguous reasons.
            _pnl_pct = (float(r['gross_pnl_rs']) / cap * 100) \
                if cap and r.get('gross_pnl_rs') is not None else None
            r['exit_bucket'] = classify_exit(r.get('exit_type'), _pnl_pct)
            _meta = r.get('metadata') if isinstance(r.get('metadata'), dict) else {}
            r['fill_status'] = _meta.get('fill_status', 'FILLED')

            # Serialize timestamps
            for key in ('entry_time', 'exit_time'):
                if r.get(key) and hasattr(r[key], 'isoformat'):
                    r[key] = r[key].isoformat()

        return jsonify({'trades': rows, 'page': page, 'has_more': has_more})
    except Exception as e:
        logger.error(f"[subscriber/trades] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/pvi/fm-trail', methods=['GET'])
def pvi_fm_trail():
    """Live-manager reasoning trail for a PVI trade — fetched from pvi_manager_comparison.

    2026-07-30: was hardcoded to agent_version='FLOW_MANAGER'. FM was disabled
    2026-07-27 (config.py PVI_FLOW_MANAGER.enabled=False, orchestrator.py
    _fm_call_enabled gate) once Shape Manager took over exclusively
    (shape_manager_leads=True) — since that restart, FM writes zero new rows,
    so this endpoint went silently empty for every trade opened after the
    cutover (confirmed: 0 FLOW_MANAGER rows for symbols entered today vs.
    100+ SHAPE_MANAGER rows). Not filtering on a hardcoded agent_version at
    all — this trail is meant to show whichever manager actually governed the
    trade, and self-heals if leadership flips again without another code
    change. agent_version is now returned per-row so the frontend can label
    entries when a trade's history spans a leadership change mid-day."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    symbol     = request.args.get('symbol', '').upper().strip()
    trade_date = request.args.get('date', '')
    if not symbol or not trade_date:
        return jsonify({'error': 'symbol and date required'}), 400
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import _exec, Candles1MinDB
        rows = _exec(
            """SELECT
                   bar_timestamp AT TIME ZONE 'Asia/Kolkata' AS bar_ist,
                   agent_version, stance, thesis, is_live,
                   ROUND(loss_ceiling_pct::numeric, 2)  AS ceil_pct,
                   ROUND(profit_lock_pct::numeric, 2)   AS lock_pct,
                   ROUND(t1_lock_pct::numeric, 2)       AS t1_pct,
                   ROUND(best_profit_pct::numeric, 2)   AS mfe_pct,
                   ROUND(current_price::numeric, 2)     AS price,
                   confidence, rationale, latency_ms, timed_out,
                   ceiling_touches, lock_activated
               FROM pvi_manager_comparison
               WHERE symbol = %s AND trade_date = %s
               ORDER BY bar_timestamp DESC""",
            (symbol, trade_date), fetch='all'
        ) or []
        for r in rows:
            if r.get('bar_ist') and hasattr(r['bar_ist'], 'strftime'):
                # Second precision (was minute-only) — bar_timestamp now carries the real
                # 1-min candle timestamp in live (2026-07-10 fix), not wall-clock-at-write,
                # so this is meaningful for spotting reasoning-staleness at a glance.
                r['bar_ist'] = r['bar_ist'].strftime('%H:%M:%S')
            for k in ('ceil_pct', 'lock_pct', 't1_pct', 'mfe_pct', 'price', 'confidence'):
                if r.get(k) is not None:
                    r[k] = float(r[k])
            if r.get('latency_ms') is not None:
                r['latency_ms'] = int(r['latency_ms'])
            if r.get('ceiling_touches') is not None:
                r['ceiling_touches'] = int(r['ceiling_touches'])
        return jsonify({'trail': rows})
    except Exception as e:
        logger.error(f'[pvi/fm-trail] {e}')
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/pvi/fm-latest', methods=['GET'])
def pvi_fm_latest():
    """Latest live-manager cycle per open PVI symbol (today). Returns
    {symbol: {lock_pct, ceil_pct, mfe_pct, stance, agent_version}}.

    2026-07-30: was hardcoded to agent_version='FLOW_MANAGER'. FM stopped
    writing rows once it was disabled 2026-07-27 (Shape Manager leads
    exclusively — see fm-trail's docstring for the full incident), so this
    endpoint returned nothing for any symbol entered since that cutover.
    Picking the single freshest row per symbol regardless of agent_version
    fixes today's gap and self-heals if leadership flips again."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import _exec, Candles1MinDB
        rows = _exec(
            """SELECT DISTINCT ON (symbol)
                   symbol, agent_version,
                   ROUND(profit_lock_pct::numeric, 2)   AS lock_pct,
                   ROUND(loss_ceiling_pct::numeric, 2)  AS ceil_pct,
                   ROUND(t1_lock_pct::numeric, 2)       AS t1_pct,
                   ROUND(best_profit_pct::numeric, 2)   AS mfe_pct,
                   ROUND(current_price::numeric, 2)     AS current_price,
                   stance, rationale,
                   ceiling_touches, lock_activated
               FROM pvi_manager_comparison
               WHERE trade_date = (NOW() AT TIME ZONE 'Asia/Kolkata')::date
               ORDER BY symbol, bar_timestamp DESC""",
            fetch='all'
        ) or []
        data = {}
        for r in rows:
            for k in ('lock_pct', 'ceil_pct', 't1_pct', 'mfe_pct', 'current_price'):
                if r.get(k) is not None:
                    r[k] = float(r[k])
            data[r['symbol']] = {k: r[k] for k in ('lock_pct', 'ceil_pct', 't1_pct', 'mfe_pct', 'current_price',
                                                      'stance', 'rationale', 'ceiling_touches', 'lock_activated',
                                                      'agent_version')}
        return jsonify({'data': data})
    except Exception as e:
        logger.error(f'[pvi/fm-latest] {e}')
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/pvi/manager-compare', methods=['GET'])
def pvi_manager_compare():
    """Live FM/Shape shadow comparison, paired by symbol and candle timestamp."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    symbol = request.args.get('symbol', '').upper().strip()
    trade_date = request.args.get('date', '')
    if not symbol or not trade_date:
        return jsonify({'error': 'symbol and date required'}), 400
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import _exec, Candles1MinDB
        rows = _exec(
            """SELECT bar_timestamp, agent_version, is_live, thesis, stance,
                      loss_ceiling_pct, profit_lock_pct, t1_lock_pct,
                      v11_stop, v11_t0, v11_action, confidence, rationale,
                      latency_ms, timed_out, current_price, bars_elapsed
                 FROM pvi_manager_comparison
                WHERE symbol=%s AND trade_date=%s
                  AND agent_version IN ('FLOW_MANAGER','SHAPE_MANAGER')
                ORDER BY bar_timestamp ASC, agent_version ASC""",
            (symbol, trade_date), fetch='all'
        ) or []
        grouped = {}
        ts_objects = {}
        candle_rows = []
        for r in rows:
            ts = r['bar_timestamp'].isoformat() if hasattr(r['bar_timestamp'], 'isoformat') else str(r['bar_timestamp'])
            ts_objects[ts] = r['bar_timestamp']
            side = 'fm' if r['agent_version'] == 'FLOW_MANAGER' else 'shape'
            item = {k: r.get(k) for k in r if k != 'bar_timestamp'}
            item['bar_timestamp'] = ts
            grouped.setdefault(ts, {})[side] = item
        # Attach the causal, partially formed 5-minute candle for every 1-minute
        # decision so the browser can show the candle exactly as it existed then.
        if ts_objects:
            min_ts = min(ts_objects.values())
            max_ts = max(ts_objects.values())
            # Keep a broad context window: 3 hours of 5-minute candles before
            # the first manager checkpoint plus the full post-entry trajectory.
            candle_rows = Candles1MinDB.get_range(
                symbol, min_ts - timedelta(minutes=180), max_ts + timedelta(minutes=61)
            )
            for ts, obj in ts_objects.items():
                # Comparison timestamps may be timestamptz while the archive is
                # deliberately UTC-naive. Normalize both before pairing.
                if getattr(obj, 'tzinfo', None) is not None:
                    from datetime import timezone as _timezone
                    obj_cmp = obj.astimezone(_timezone.utc).replace(tzinfo=None)
                else:
                    obj_cmp = obj
                bucket_min = (obj.minute // 5) * 5
                bucket_start = obj_cmp.replace(minute=bucket_min, second=0, microsecond=0)
                cs = [c for c in candle_rows if bucket_start <= c['timestamp'] <= obj_cmp]
                if cs:
                    grouped[ts]['candle_5m'] = {
                        'bucket_start': bucket_start.isoformat() + ('+00:00' if bucket_start.tzinfo is None else ''),
                        'minutes_in_candle': obj.minute - bucket_min + 1,
                        'open': float(cs[0]['open']),
                        'high': max(float(c['high']) for c in cs),
                        'low': min(float(c['low']) for c in cs),
                        'close': float(cs[-1]['close']),
                        'volume': sum(int(c['volume'] or 0) for c in cs),
                    }
        trajectory = [{
            'timestamp': c['timestamp'].isoformat() + ('+00:00' if c['timestamp'].tzinfo is None else ''),
            'open': float(c['open']), 'high': float(c['high']),
            'low': float(c['low']), 'close': float(c['close']),
            'volume': int(c.get('volume') or 0),
        } for c in candle_rows]
        markers = []
        # Entry and real tranche-leg timestamps are stored on the PVI trade.
        trade_rows = _exec(
            """SELECT entry_time, exit_time, entry_price, metadata,
                          metadata->'tranche_fill_log' AS tranche_fill_log
                 FROM trades_v2
                WHERE strategy_id='PVI' AND symbol=%s
                  AND (entry_time AT TIME ZONE 'Asia/Kolkata')::date=%s
                ORDER BY entry_time""",
            (symbol, trade_date), fetch='all'
        ) or []
        for tr in trade_rows:
            meta = tr.get('metadata') or {}
            for key, label in (('signal_time', 'SIGNAL'), ('signal_candle_ts', 'SIGNAL')):
                if meta.get(key):
                    markers.append({'type': label, 'timestamp': str(meta[key])})
            if tr.get('entry_time'):
                markers.append({'type': 'ENTRY', 'timestamp': tr['entry_time'].isoformat()})
            if tr.get('exit_time'):
                markers.append({'type': 'EXIT', 'timestamp': tr['exit_time'].isoformat()})
            for leg in (tr.get('tranche_fill_log') or []):
                if leg.get('ts'):
                    markers.append({'type': 'TRANCHE', 'timestamp': leg['ts'], 'leg': leg.get('leg')})
        # Expose absolute T1 prices to the chart. Shape stores its raw absolute
        # levels in rationale; FM stores the percentage and the trade supplies
        # the entry anchor.
        #
        # entry_price is attached to BOTH sides (not just shape) so the frontend can
        # derive FM's real S0/T0 from loss_ceiling_pct/profit_lock_pct — v11_stop/
        # v11_t0 are NOT reliable for the FM row: they're sourced from legacy
        # LevelManager-era fields (cur_stop / t0_px, strategies/pvi.py ~2394-2395),
        # not the FM's actual dynamic ceiling/lock. loss_ceiling_pct/profit_lock_pct
        # are the FM's real values and were already being fetched but unused here.
        # v11_stop/v11_t0 ARE correct for the SHAPE_MANAGER row (db/queries.py
        # write_shape sets both v11_t0 and profit_lock_pct from the same shape
        # t0_price), so shape's own display isn't affected by this.
        _entry_price = next(
            (float(t.get('entry_price')) for t in trade_rows if t.get('entry_price') is not None), None
        )
        for ts, pair in grouped.items():
            shape_item = pair.get('shape') or {}
            fm_item = pair.get('fm') or {}
            if _entry_price is not None:
                fm_item['entry_price'] = _entry_price
                shape_item['entry_price'] = _entry_price
            try:
                raw = json.loads(shape_item.get('rationale') or '{}').get('raw_levels') or {}
                shape_item['t1_price'] = raw.get('t1_price')
            except Exception:
                pass
        return jsonify({'symbol': symbol, 'trade_date': trade_date,
                        'rows': [{'bar_timestamp': ts, **v} for ts, v in grouped.items()],
                        'trajectory_1m': trajectory, 'markers': markers})
    except Exception as e:
        logger.error(f'[pvi/manager-compare] {e}')
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/pvi/manager-symbols', methods=['GET'])
def pvi_manager_symbols():
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    trade_date = request.args.get('date', '')
    if not trade_date:
        return jsonify({'symbols': []})
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import _exec
        rows = _exec(
            """SELECT DISTINCT symbol FROM pvi_manager_comparison
                WHERE trade_date=%s ORDER BY symbol""",
            (trade_date,), fetch='all'
        ) or []
        return jsonify({'symbols': [r['symbol'] for r in rows]})
    except Exception as e:
        logger.error(f'[pvi/manager-symbols] {e}')
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/pvi/today-trades', methods=['GET'])
def pvi_today_trades():
    """Latest trading day's PVI trades (live + closed), minimal fields for the
    dashboard's live-chart tab bar. Deliberately unconditional (not gated by
    which Positions sub-tab is open) so a 30s poll never goes stale.

    Scoped to MAX(trade_date) rather than IST-calendar-today: a literal
    "today" filter goes stale the instant midnight IST rolls over even though
    the market doesn't reopen until 09:15 IST — same bug class already fixed
    in /api/pvi/entry-shadow-latest. This way the last trading session's
    trades (including its closed ones) stay visible overnight and only roll
    over once the next session's first trade actually lands."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import _exec
        rows = _exec(
            """SELECT trade_id::text AS id, symbol,
                      CAST(capital_deployed AS FLOAT) AS capital_deployed,
                      CAST(entry_price AS FLOAT) AS entry_price,
                      CAST(exit_price  AS FLOAT) AS exit_price,
                      entry_time, exit_time
                 FROM trades_v2
                WHERE strategy_id = 'PVI'
                  AND trade_date = (SELECT MAX(trade_date) FROM trades_v2 WHERE strategy_id = 'PVI')
                ORDER BY capital_deployed DESC NULLS LAST""",
            fetch='all'
        ) or []
        for r in rows:
            r['is_live'] = r.get('exit_time') is None
            # PVI is SHORT-only: profitable when exit < entry. Lets the tab bar
            # color-code closed trades without a second round trip.
            r['profitable'] = (
                r['exit_price'] < r['entry_price']
                if (not r['is_live'] and r.get('exit_price') is not None and r.get('entry_price') is not None)
                else None
            )
            for k in ('entry_time', 'exit_time'):
                if r.get(k) and hasattr(r[k], 'isoformat'):
                    r[k] = r[k].isoformat()
        # Whether Shape Manager is the one actually governing live stops right
        # now, vs just shadow-logging alongside Flow Manager — this flag is
        # runtime-toggleable (strategy_controls.shape_manager_leads) and has
        # already flipped once without the dashboard label being updated to
        # match, so read it fresh each poll rather than hardcoding a label.
        ctrl = _exec(
            "SELECT shape_manager_leads FROM strategy_controls WHERE strategy_id = 'PVI'",
            fetch='all'
        ) or []
        shape_manager_leads = bool(ctrl[0]['shape_manager_leads']) if ctrl else False
        return jsonify({'trades': rows, 'shape_manager_leads': shape_manager_leads})
    except Exception as e:
        logger.error(f'[pvi/today-trades] {e}')
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/pvi/live-chart', methods=['GET'])
def pvi_live_chart():
    """Full-day 1-min candles + Shape Manager (shadow) band history + tranche/exit
    markers for a single PVI trade, keyed by trade_id (a symbol can have >1 same-day
    PVI trade, e.g. a Round-2 recovery re-entry, so symbol+date isn't unique enough).

    S0/T0/T1 are returned as raw pct columns (loss_ceiling_pct/profit_lock_pct/
    t1_lock_pct) + entry_price — NOT from rationale->raw_levels, which is the
    Shape prompt's pre-hybrid, stale model output (see pvi_shape_compare_actual.py::
    live_shape() vs orchestrator's hybrid_shape_levels() overwrite). The frontend
    derives absolute prices itself: entry_price * (1 + pct/100).

    Known small inaccuracy for tranche-entry trades: write_shape() computed each
    pct at bar-write time against that minute's THEN-CURRENT p2_snap entry_price,
    which for tranche trades drifts as legs fill and blended cost updates (see
    apply_tranche_leg_fill's blended_price re-anchor). Only the trade's FINAL
    entry_price is persisted, so reconstructing early-bar bands with it instead
    of each bar's true anchor introduces error bounded by the leg-fill price
    spread — measured ~0.07-0.08% on a real tranche trade (ATALREAL 2026-07-24),
    negligible and self-corrects once fills complete. Exact reconstruction would
    need a per-bar entry_price column that doesn't exist today."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    trade_id = request.args.get('trade_id', '').strip()
    if not trade_id:
        return jsonify({'error': 'trade_id required'}), 400
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from datetime import time as _dtime
        from db.queries import _exec, Candles1MinDB
        trades = _exec(
            """SELECT trade_id::text AS id, symbol, entry_time, exit_time,
                      CAST(entry_price AS FLOAT) AS entry_price,
                      CAST(exit_price  AS FLOAT) AS exit_price,
                      CAST(capital_deployed AS FLOAT) AS capital_deployed,
                      trade_date,
                      metadata->'tranche_fill_log' AS tranche_fill_log
                 FROM trades_v2
                WHERE trade_id = %s AND strategy_id = 'PVI'""",
            (trade_id,), fetch='all'
        ) or []
        if not trades:
            return jsonify({'error': 'trade not found'}), 404
        tr = trades[0]
        symbol = tr['symbol']
        trade_date = tr['trade_date']
        is_live = tr['exit_time'] is None

        # trade_date is IST calendar date; archive is UTC-naive, so
        # 09:15 IST -> 03:45 UTC and 15:30 IST -> 10:00 UTC on that same date.
        day_open  = datetime.combine(trade_date, _dtime(3, 45))
        day_close = datetime.combine(trade_date, _dtime(10, 0))
        end = min(datetime.utcnow(), day_close) if is_live else day_close

        raw_candles = Candles1MinDB.get_range(symbol, day_open, end + timedelta(minutes=1))
        candles_1m = [{
            'timestamp': c['timestamp'].isoformat(),
            'open': float(c['open']), 'high': float(c['high']),
            'low': float(c['low']), 'close': float(c['close']),
            'volume': int(c.get('volume') or 0),
        } for c in raw_candles]

        # Only the three band columns are used by the chart — confidence/stance/
        # ceiling_touches/lock_activated are shown elsewhere (FM trail modal, a
        # different endpoint) and were dead weight on this payload.
        sm_rows_raw = _exec(
            """SELECT bar_timestamp, loss_ceiling_pct, profit_lock_pct, t1_lock_pct
                 FROM pvi_manager_comparison
                WHERE symbol = %s AND trade_date = %s AND agent_version = 'SHAPE_MANAGER'
                ORDER BY bar_timestamp ASC""",
            (symbol, trade_date), fetch='all'
        ) or []
        sm_rows = []
        for r in sm_rows_raw:
            sm_rows.append({
                'bar_timestamp': r['bar_timestamp'].isoformat(),
                'loss_ceiling_pct': float(r['loss_ceiling_pct']) if r.get('loss_ceiling_pct') is not None else None,
                'profit_lock_pct':  float(r['profit_lock_pct'])  if r.get('profit_lock_pct')  is not None else None,
                't1_lock_pct':      float(r['t1_lock_pct'])      if r.get('t1_lock_pct')      is not None else None,
            })

        tranche_markers = []
        for leg in (tr.get('tranche_fill_log') or []):
            if not isinstance(leg, dict) or leg.get('price') is None:
                continue
            tranche_markers.append({
                'ts': leg.get('ts'), 'leg': leg.get('leg'), 'price': float(leg['price']),
            })

        return jsonify({
            'trade_id': tr['id'], 'symbol': symbol,
            'trade_date': trade_date.isoformat(), 'is_live': is_live,
            'entry_time':  tr['entry_time'].isoformat()  if tr.get('entry_time')  else None,
            'entry_price': tr.get('entry_price'),
            'exit_time':   tr['exit_time'].isoformat()   if tr.get('exit_time')   else None,
            'exit_price':  tr.get('exit_price'),
            'capital_deployed': tr.get('capital_deployed'),
            'day_open':  day_open.isoformat() + 'Z',
            'day_close': day_close.isoformat() + 'Z',
            'candles_1m': candles_1m,
            'sm_rows': sm_rows,
            'tranche_markers': tranche_markers,
        })
    except Exception as e:
        logger.error(f'[pvi/live-chart] {e}')
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/pvi/shape-rerun', methods=['GET'])
def pvi_shape_rerun():
    """Run the current Shape prompt at one historical checkpoint; FM untouched."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    symbol = request.args.get('symbol', '').upper().strip()
    checkpoint = request.args.get('timestamp', '')
    if not symbol or not checkpoint:
        return jsonify({'error': 'symbol and timestamp required'}), 400
    try:
        from datetime import timezone as _timezone
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import _exec, Candles1MinDB
        from tools.pvi_shape_compare_actual import shape_context, live_shape, hybrid_shape_levels
        target = datetime.fromisoformat(checkpoint.replace('Z', '+00:00'))
        if target.tzinfo is None:
            target = target.replace(tzinfo=_timezone.utc)
        else:
            target = target.astimezone(_timezone.utc)
        trades = _exec(
            """SELECT entry_time, entry_price, stop_price, metadata
                 FROM trades_v2 WHERE strategy_id='PVI' AND symbol=%s
                   AND entry_time <= %s
                 ORDER BY entry_time DESC LIMIT 1""",
            (symbol, target), fetch='all'
        ) or []
        if not trades:
            return jsonify({'error': 'trade anchor not found'}), 404
        tr = trades[0]
        entry_time = tr['entry_time']
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=_timezone.utc)
        else:
            entry_time = entry_time.astimezone(_timezone.utc)
        raw = Candles1MinDB.get_range(symbol, entry_time - timedelta(minutes=45), target + timedelta(minutes=1))
        candles = []
        for c in raw:
            x = dict(c); ts = x['timestamp']
            if ts.tzinfo is None: ts = ts.replace(tzinfo=_timezone.utc)
            else: ts = ts.astimezone(_timezone.utc)
            if ts > target: continue
            x['timestamp'] = ts
            for k in ('open', 'high', 'low', 'close'): x[k] = float(x[k])
            for k in ('volume', 'buy_vol', 'sell_vol'): x[k] = int(x.get(k) or 0)
            candles.append(x)
        entry_i = next((i for i, c in enumerate(candles) if c['timestamp'] >= entry_time), None)
        if entry_i is None or len(candles) < 2:
            return jsonify({'error': 'insufficient candles for checkpoint'}), 422
        meta = tr.get('metadata') or {}
        fill_log = meta.get('tranche_fill_log') or []
        fills_by_target = []
        for leg in fill_log:
            if not isinstance(leg, dict):
                continue
            ts = leg.get('ts') or leg.get('timestamp')
            try:
                if ts and datetime.fromisoformat(str(ts).replace('Z', '+00:00')).astimezone(_timezone.utc) <= target:
                    fills_by_target.append(leg)
            except Exception:
                fills_by_target.append(leg)
        is_tranche = bool(meta.get('is_tranche_entry') or meta.get('entry_tranche_swap'))
        legs_filled = max(1, len(fills_by_target)) if is_tranche else 0
        fill_ctx = {
            'is_tranche_entry': is_tranche,
            'legs_filled': legs_filled,
            'legs_planned': 6,
            'fill_phase_active': bool(is_tranche and legs_filled < 6 and (len(candles) - entry_i) <= 20),
            'trigger_legs': 6,
        }
        prior_state = {}
        try:
            prior_state = json.loads(request.args.get('state') or '{}')
        except Exception:
            prior_state = {}
        ctx = shape_context(candles, len(candles) - 1, entry_i, float(tr['entry_price']), fill_ctx, prior_state)
        requested_model = request.args.get('model', '').strip()
        rerun_model = requested_model if requested_model in ('deepseek-chat', 'deepseek-reasoner') else os.environ.get('PVI_SHAPE_MODEL', 'deepseek-chat')
        result = live_shape(ctx, model=rerun_model)
        # Browser reruns must use the same hybrid contract as live shadow:
        # Shape supplies regime/context, while the mechanical band layer owns
        # exact T1/T0/S0 geometry and the 1–3% convergence constraints.
        result.update(hybrid_shape_levels(ctx, result))
        result['rerun'] = True
        result['checkpoint'] = target.isoformat()
        return jsonify({'symbol': symbol, 'timestamp': target.isoformat(), 'shape': result})
    except Exception as e:
        logger.exception('[pvi/shape-rerun] failed')
        return jsonify({'error': str(e)}), 500


@app.route('/pvi/manager-compare', methods=['GET'])
def pvi_manager_compare_page():
    """Formatted one-click FM vs Shape 5-minute/1-minute comparison viewer."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return redirect('/login')
    return render_template_string(r'''<!doctype html>
<html><head><meta charset="utf-8"><title>PVI FM vs Shape</title>
<style>
body{font:14px system-ui;background:#10141c;color:#e8edf5;margin:18px}h1{font-size:21px;margin:0 0 10px}input,button{background:#1c2533;color:#fff;border:1px solid #40506a;padding:7px 9px;border-radius:6px}button{cursor:pointer;background:#2764b8}.bar{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.nav{display:flex;gap:8px;align-items:center;margin:10px 0}.muted{color:#9aa8bc}.layout{display:grid;grid-template-columns:minmax(0,1fr) 285px;gap:12px}.panel{border:1px solid #2c3748;border-radius:8px;padding:12px;background:#141b26}.chartbox{background:#0c1118;border-radius:6px;overflow:hidden}.big{font-size:17px;font-weight:700;margin-bottom:8px}canvas{display:block;width:100%;height:min(68vh,620px)}.side h3{margin:5px 0 8px;font-size:15px}.side table{border-collapse:collapse;width:100%;margin-bottom:13px}td,th{padding:6px 4px;border-bottom:1px solid #303c4e;text-align:right}td:first-child,th:first-child{text-align:left}.fm{color:#f0a52b}.sm{color:#31d6c2}.action{padding:8px;background:#1a2330;border-radius:6px;margin-bottom:10px;line-height:1.55}.legend{display:flex;gap:12px;margin-top:7px;font-size:12px}.hint{font-size:12px;color:#9aa8bc}.rerunCell,.rerunHead{color:#e58cff!important}.side th.fm{color:#f0a52b!important}.side th.sm{color:#31d6c2!important}
</style><style>table{table-layout:fixed;font-size:12px;overflow-wrap:anywhere}th,td{max-width:25%;white-space:normal;overflow-wrap:anywhere} .side{min-width:0;overflow:hidden}</style></head><body><h1>PVI — FM vs Shape · 5-minute candle / 1-minute updates</h1>
<div class="bar"><label>Instrument <select id="sym"><option>CONFIPET</option></select></label><label>Date <input id="date" type="date" onchange="loadSymbols()"></label><button onclick="load()">Load</button><span id="status" class="muted"></span></div>
<div class="nav"><button onclick="setMode('step')">Step / Replay</button><button onclick="setMode('overview')">Full Overview</button><button onclick="move(-1)">← Back</button><button onclick="move(1)">Next →</button><button onclick="runSelectedRerun()" style="background:#7d3ca8">Rerun selected candle</button><span id="modeLabel" class="muted">Step / Replay</span><span id="counter" class="muted"></span><span id="title" class="big"></span></div>
<div class="layout"><div class="panel"><div class="hint">3-hour context view · 36 anchored 5-minute candles · each selected checkpoint updates the current candle from 1-minute data</div><div class="chartbox"><canvas id="chart" width="1200" height="620"></canvas></div><div class="legend"><span class="fm">■ FM levels</span><span class="sm">■ Stored SM</span><span style="color:#e58cff">■ Rerun SM</span><span>▌ selected 5m candle</span></div></div><div class="panel side"><h3>Selected checkpoint</h3><div id="checkpoint" class="action"></div><h3 class="fm">Flow Manager</h3><div id="fm" class="action"></div><h3 class="sm">Stored Shape Manager</h3><div id="sm" class="action"></div><h3 style="color:#e58cff">Rerun Shape Manager</h3><div id="rerun" class="action">Step / Replay to run the revised Shape prompt.</div><table><thead><tr><th>Level</th><th class="fm">FM</th><th class="sm">Stored SM</th><th class="rerunHead">Rerun SM</th></tr></thead><tbody id="diff"></tbody></table></div></div>
<script>
const $=id=>document.getElementById(id); let data=[],baseRows=[],idx=0,trajectory=[],markers=[],showFuture=false,mode='step';
function dt(v){let s=String(v);if(!/[zZ]|[+-]\d\d:\d\d$/.test(s))s+='Z';return new Date(s)}
const _istTime=Date.prototype.toLocaleTimeString; Date.prototype.toLocaleTimeString=function(locales,opts){return _istTime.call(this,locales,Object.assign({},opts||{},{timeZone:'Asia/Kolkata'}))};
function fmt(v){return v==null?'—':(typeof v==='number'?v.toFixed(2):v)}
function t1px(x){if(!x)return null;if(x.t1_price!=null)return +x.t1_price;if(x.entry_price!=null&&x.t1_lock_pct!=null)return +x.entry_price*(1+ +x.t1_lock_pct/100);return null}
// s0px/t0px: derive absolute price from loss_ceiling_pct/profit_lock_pct + entry_price,
// NOT v11_stop/v11_t0 (those are the FM row's legacy cur_stop/t0_px fields, pre-dating
// the Flow Manager and no longer what drives real exits — see backend comment above).
function s0px(x){if(!x)return null;if(x.entry_price!=null&&x.loss_ceiling_pct!=null)return +x.entry_price*(1+ +x.loss_ceiling_pct/100);return null}
function t0px(x){if(!x)return null;if(x.entry_price!=null&&x.profit_lock_pct!=null)return +x.entry_price*(1+ +x.profit_lock_pct/100);return null}
function smS0(x){if(!x)return null;if(x.s0_price!=null)return +x.s0_price;return s0px(x)}
function smT0(x){if(!x)return null;if(x.t0_price!=null)return +x.t0_price;return t0px(x)}
function pct(x){return x==null?'—':fmt(x)+'%'}
function rerunEnd(q,r){if(!q||!r)return '';let c=candleAt(r);if(q.action==='EXIT_CONFIRM')return '<div style="color:#ff8a8a"><b>SM trade end: EXIT_CONFIRM</b></div>';if(c){if(q.s0_price!=null&&c.high>=+q.s0_price)return '<div style="color:#ff8a8a"><b>SM trade end: S0 stop</b></div>';if(q.t1_price!=null&&c.low<=+q.t1_price)return '<div style="color:#8ff0c5"><b>SM trade end: T1 profit</b></div>';if(q.t0_price!=null&&c.low<=+q.t0_price)return '<div style="color:#8ff0c5"><b>SM trade end: T0 profit</b></div>'}return '<div class="muted">SM trade end: not reached at this checkpoint</div>'}
function levels(x,shape=false){if(!x)return '<span class="muted">no output</span>';let s=shape?smS0(x):s0px(x),t=shape?smT0(x):t0px(x);return `<div><b>${x.v11_action||'—'}</b> · ${x.thesis||'—'} / ${x.stance||'—'}</div><div>S0 <b>${fmt(s)}</b> · T0 <b>${fmt(t)}</b></div><div>T1 ${x.t1_price!=null?fmt(x.t1_price):pct(x.t1_lock_pct)} · conf ${pct(x.confidence)} · ${x.latency_ms||0}ms</div>`}
function visibleCandles(){let end=showFuture?null:(data[idx]&&new Date(data[idx].bar_timestamp)),m=new Map();trajectory.filter(c=>!end||new Date(c.timestamp)<=end).forEach(c=>{let d=new Date(c.timestamp),bs=new Date(d);bs.setUTCMinutes(Math.floor(bs.getUTCMinutes()/5)*5,0,0);let k=bs.toISOString();let x=m.get(k);if(!x)x={bucket_start:k,minutes_in_candle:0,open:c.open,high:c.high,low:c.low,close:c.close,volume:0};x.high=Math.max(x.high,c.high);x.low=Math.min(x.low,c.low);x.close=c.close;x.volume+=c.volume||0;x.minutes_in_candle=Math.min(5,x.minutes_in_candle+1);m.set(k,x)});return mode==='overview'?[...m.values()]:[...m.values()].slice(-36)}
function candleAt(r){if(r?.candle_5m)return r.candle_5m;let end=r&&dt(r.bar_timestamp);if(!end)return null;let b=new Date(end);b.setUTCMinutes(Math.floor(b.getUTCMinutes()/5)*5,0,0);let a=trajectory.filter(c=>{let d=dt(c.timestamp),z=new Date(d);z.setUTCMinutes(Math.floor(z.getUTCMinutes()/5)*5,0,0);return z.getTime()===b.getTime()&&d<=end});if(!a.length)return null;return {bucket_start:b.toISOString(),minutes_in_candle:a.length,open:a[0].open,high:Math.max(...a.map(c=>c.high)),low:Math.min(...a.map(c=>c.low)),close:a[a.length-1].close}}
function pairAt(i){let f=null,s=null;for(let k=Math.min(i,data.length-1);k>=0;k--){if(!f&&data[k]?.fm)f=data[k].fm;if(!s&&data[k]?.shape)s=data[k].shape;if(f&&s)break}return {fm:f||{},shape:s||{}}}
function draw(){let cv=$('chart'),g=cv.getContext('2d'),W=cv.width,H=cv.height;g.clearRect(0,0,W,H);let cs=visibleCandles(),r=data[idx],sel=candleAt(r);if(!cs.length){g.fillStyle='#9aa8bc';g.fillText('No candle data returned by API',30,40);return}let vals=cs.flatMap(c=>[c.open,c.high,c.low,c.close]),p=pairAt(idx),f=p.fm,s=p.shape,q=mode==='step'?rerunShape:null,fS0=s0px(f),fT0=t0px(f),fT1=t1px(f),sS0=smS0(s),sT0=smT0(s),sT1=t1px(s);[fS0,fT0,fT1,sS0,sT0,sT1,q?.s0_price,q?.t0_price,q?.t1_price].forEach(v=>{if(v!=null)vals.push(+v)});let lo=Math.min(...vals),hi=Math.max(...vals),pad=(hi-lo||1)*.12,left=62,right=20,top=28,bottom=32,plotH=H-top-bottom,plotW=W-left-right,y=v=>top+(hi+pad-v)/(hi-lo+2*pad)*plotH;g.font='13px system-ui';for(let k=0;k<6;k++){let v=lo+((hi-lo)*k/5),yy=y(v);g.strokeStyle='#263344';g.beginPath();g.moveTo(left,yy);g.lineTo(W-right,yy);g.stroke();g.fillStyle='#b8c4d4';g.fillText(fmt(v),6,yy+4)}let slot=plotW/Math.max(cs.length,1);cs.forEach((c,i)=>{let x=left+slot*(i+.5),up=c.close>=c.open;g.strokeStyle=up?'#52c7a3':'#e66b75';g.beginPath();g.moveTo(x,y(c.high));g.lineTo(x,y(c.low));g.stroke();g.fillStyle=up?'#52c7a3':'#e66b75';let yy=Math.min(y(c.open),y(c.close)),hh=Math.max(3,Math.abs(y(c.close)-y(c.open)));g.fillRect(x-slot*.25,yy,slot*.5,hh);g.fillStyle='#9aa8bc';g.fillText(new Date(c.bucket_start).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}),x-slot*.35,H-10);if(sel&&c.bucket_start===sel.bucket_start){g.strokeStyle='#fff';g.lineWidth=2;g.strokeRect(x-slot*.3,top,slot*.6,plotH);g.lineWidth=1}});function line(v,color,label){if(v==null)return;let yy=y(+v);g.strokeStyle=color;g.setLineDash([6,4]);g.beginPath();g.moveTo(left,yy);g.lineTo(W-right,yy);g.stroke();g.setLineDash([]);g.fillStyle=color;g.fillText(label+' '+fmt(v),W-right-145,yy-4)}line(fS0,'#f0a52b','FM S0');line(fT0,'#f0a52b','FM T0');line(fT1,'#f0a52b','FM T1');line(sS0,'#31d6c2','Stored SM S0');line(sT0,'#31d6c2','Stored SM T0');line(sT1,'#31d6c2','Stored SM T1');line(q?.s0_price,'#e58cff','Rerun SM S0');line(q?.t0_price,'#e58cff','Rerun SM T0');line(q?.t1_price,'#e58cff','Rerun SM T1');let endTs=r&&new Date(r.bar_timestamp),visibleTraj=trajectory.filter(c=>!endTs||new Date(c.timestamp)<=endTs),loTs=visibleTraj.length?new Date(visibleTraj[0].timestamp):null,hiTs=visibleTraj.length?new Date(visibleTraj[visibleTraj.length-1].timestamp):null;markers.forEach(m=>{let t=new Date(m.timestamp);if(loTs&&hiTs&&t>=loTs&&t<=hiTs){let p=(t-loTs)/(hiTs-loTs||1),x=left+p*plotW;g.strokeStyle=m.type==='ENTRY'?'#fff':'#d88cff';g.setLineDash([3,5]);g.beginPath();g.moveTo(x,top);g.lineTo(x,top+plotH);g.stroke();g.setLineDash([]);g.fillStyle=g.strokeStyle;g.fillText(m.type+(m.leg?' '+m.leg:''),Math.min(x+3,W-75),top+14)}});g.fillStyle='#e8edf5';g.fillText(`5m ${sel?sel.minutes_in_candle+'/5 min':''} · O ${fmt(sel?.open)} H ${fmt(sel?.high)} L ${fmt(sel?.low)} C ${fmt(sel?.close)}`,left,18)}
const _drawBase=draw; draw=function(){const saved=markers;markers=[];_drawBase();markers=saved;const cs=visibleCandles();if(!cs.length)return;const cv=$('chart'),g=cv.getContext('2d'),W=cv.width,H=cv.height,left=62,right=20,top=28,plotW=W-left-right,lo=dt(cs[0].bucket_start),hi=new Date(dt(cs[cs.length-1].bucket_start).getTime()+300000);markers.forEach(m=>{const t=dt(m.timestamp);if(t<lo||t>hi)return;const x=left+(t-lo)/(hi-lo||1)*plotW;g.strokeStyle=m.type==='ENTRY'?'#fff':m.type==='EXIT'?'#ff6b6b':'#d88cff';g.setLineDash([3,5]);g.beginPath();g.moveTo(x,top);g.lineTo(x,H-32);g.stroke();g.setLineDash([]);g.fillStyle=g.strokeStyle;g.fillText(m.type+(m.leg?' '+m.leg:''),Math.min(x+3,W-78),top+14)})};
function select(i){if(!data.length)return;idx=Math.max(0,Math.min(data.length-1,i));let r=data[idx],f=r.fm||{},s=r.shape||{},c=candleAt(r);$('counter').textContent=`${idx+1} / ${data.length}`;$('title').textContent=`${$('sym').value} · ${new Date(r.bar_timestamp).toLocaleTimeString()}`;$('checkpoint').innerHTML=`${new Date(r.bar_timestamp).toLocaleTimeString()}${r.synthetic?' · post-exit replay':''} · candle ${c?.minutes_in_candle||'?'} / 5 minutes`;draw();$('fm').innerHTML=levels(f);$('sm').innerHTML=levels(s);$('diff').innerHTML=[['S0',fmt(s0px(f)),fmt(s0px(s))],['T0',fmt(t0px(f)),fmt(t0px(s))],['T1',fmt(t1px(f)),fmt(t1px(s))],['Action',f.v11_action||'—',s.v11_action||'—'],['Confidence',pct(f.confidence),pct(s.confidence)]].map(x=>`<tr><td>${x[0]}</td><td>${x[1]}</td><td>${x[2]}</td></tr>`).join('')}
let rerunShape=null,rerunToken=0,rerunStateByIndex={};
function renderRerun(){let p=pairAt(idx),f=p.fm,s=p.shape,q=rerunShape||{};$('rerun').innerHTML=rerunShape?levels(q,true)+rerunEnd(q,data[idx]):'Running revised Shape prompt…';$('diff').innerHTML=[['S0',fmt(s0px(f)),fmt(smS0(s)),fmt(q.s0_price)],['T0',fmt(t0px(f)),fmt(smT0(s)),fmt(q.t0_price)],['T1',fmt(t1px(f)),fmt(t1px(s)),fmt(q.t1_price)],['Action',f.v11_action||'—',s.v11_action||'—',q.action||'—'],['Confidence',pct(f.confidence),pct(s.confidence),pct((q.confidence||0)*100)]].map(x=>`<tr><td>${x[0]}</td><td>${x[1]}</td><td>${x[2]}</td><td class="rerunCell">${x[3]}</td></tr>`).join('');draw()}
async function runRerun(row,priorState={}){if(!row||mode!=='step')return;let token=++rerunToken;rerunShape=null;renderRerun();let state=encodeURIComponent(JSON.stringify(priorState||{}));let u=`/api/pvi/shape-rerun?symbol=${encodeURIComponent($('sym').value)}&timestamp=${encodeURIComponent(row.bar_timestamp)}&state=${state}`;try{let res=await fetch(u),j=await res.json();if(token!==rerunToken)return;if(!res.ok)throw new Error(j.error||'rerun failed');rerunShape=j.shape;rerunStateByIndex[idx]=j.shape;renderRerun()}catch(e){if(token===rerunToken)$('rerun').innerHTML=`<span style="color:#ff8a8a">${e.message}</span>`}}
function rerunStateBefore(i){let p=rerunStateByIndex[i-1];if(!p)return {};return {previous_checkpoint:data[i-1]?.bar_timestamp||null,previous_shape:p.five_min_shape||null,previous_action:p.action||null,previous_levels:{t1_price:p.t1_price??null,t0_price:p.t0_price??null,s0_price:p.s0_price??null},level_revision:p.shape_revision||null,level_validation:p.level_validation||null,instruction:'Update these active levels; do not reset them without evidence from the new geometry.'}}
function runSelectedRerun(){if(!data.length||mode!=='step')return;runRerun(data[idx],rerunStateBefore(idx))}
const _selectBase=select; select=function(i){showFuture=mode==='overview';_selectBase(i);rerunShape=null;renderRerun()};
const _selectFrozen=select; select=function(i){_selectFrozen(i);let p=pairAt(idx);$('fm').innerHTML=levels(p.fm,false);$('sm').innerHTML=levels(p.shape,true);renderRerun()};
function setMode(m){mode=m;showFuture=m==='overview';$('modeLabel').textContent=m==='overview'?'Full Overview':'Step / Replay';if(data.length)select(idx)}
function overlayLevels(){let cs=visibleCandles(),r=data[idx];if(!cs.length||!r)return;let cv=$('chart'),g=cv.getContext('2d'),W=cv.width,H=cv.height,left=62,right=20,top=28,bottom=32,plotH=H-top-bottom,vals=cs.flatMap(c=>[c.open,c.high,c.low,c.close]),p=pairAt(idx),f=p.fm,s=p.shape,q=mode==='step'?rerunShape:null;[s0px(f),t0px(f),t1px(f),s0px(s),t0px(s),t1px(s),q?.s0_price,q?.t0_price,q?.t1_price].forEach(v=>{if(v!=null)vals.push(+v)});let lo=Math.min(...vals),hi=Math.max(...vals),pad=(hi-lo||1)*.12,y=v=>top+(hi+pad-v)/(hi-lo+2*pad)*plotH;function ln(v,color,label){if(v==null)return;let yy=y(+v);g.strokeStyle=color;g.setLineDash([6,4]);g.beginPath();g.moveTo(left,yy);g.lineTo(W-right,yy);g.stroke();g.setLineDash([]);g.fillStyle=color;g.fillText(label+' '+fmt(v),W-right-125,yy-4)}ln(s0px(f),'#efb04b','FM S0');ln(t0px(f),'#efb04b','FM T0');ln(t1px(f),'#efb04b','FM T1');ln(s0px(s),'#52c7a3','SM S0');ln(t0px(s),'#52c7a3','SM T0');ln(t1px(s),'#52c7a3','SM T1');ln(q?.s0_price,'#d88cff','Rerun S0');ln(q?.t0_price,'#d88cff','Rerun T0');ln(q?.t1_price,'#d88cff','Rerun T1')}
const _drawWithLevels=draw;draw=function(){_drawWithLevels()};
function move(n){select(idx+n)}
async function load(){let sym=$('sym').value.trim().toUpperCase(),date=$('date').value;if(!sym||!date)return;$('status').textContent='Loading…';let rr=await fetch(`/api/pvi/manager-compare?symbol=${encodeURIComponent(sym)}&date=${date}`),j=await rr.json();baseRows=j.rows||[];trajectory=j.trajectory_1m||[];markers=j.markers||[];rerunStateByIndex={};let last=baseRows.length?dt(baseRows[baseRows.length-1].bar_timestamp):null;let post=last?trajectory.filter(c=>dt(c.timestamp)>last).map(c=>({bar_timestamp:c.timestamp,synthetic:true,candle_5m:null})):[];data=baseRows.concat(post);idx=0;$('status').textContent=j.error||`${baseRows.length} checkpoints · ${trajectory.length} 1m candles · ${post.length} replay checkpoints`;if(data.length)select(idx);else{$('counter').textContent='';$('checkpoint').textContent='No comparison rows'}}
async function loadSymbols(){let date=$('date').value;if(!date)return;let r=await fetch(`/api/pvi/manager-symbols?date=${date}`),j=await r.json(),old=$('sym').value;let syms=j.symbols||[];$('sym').innerHTML=syms.map(s=>`<option>${s}</option>`).join('');if(syms.includes(old))$('sym').value=old}
document.addEventListener('keydown',e=>{if(e.key==='ArrowLeft')move(-1);if(e.key==='ArrowRight')move(1)});
$('date').value=new Date().toISOString().slice(0,10);loadSymbols().then(load);
</script></body></html>''')


@app.route('/api/pvi_eod/fm-latest', methods=['GET'])
def pvi_eod_fm_latest():
    """Latest FM decision per open PVI_EOD symbol, sourced from trades_v2.metadata
    (._fm_eod_log, patched every FM call — see strategies/pvi_eod.py). Unlike PVI
    intraday, pvi_eod_manager_comparison never captured take_profit_pct/
    loss_ceiling_pct (S0/T1 postdate that table's schema), so metadata is the only
    complete source. Returns {symbol: {trail_retrace_pct (T0), take_profit_pct (T1),
    loss_ceiling_pct (S0), mfe_pct, pnl_pct, entry_price, posture, confidence,
    rationale}} for LONG-side display (all levels expressed relative to entry)."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import _exec
        from strategies.pvi_eod_spec import PVIEODSpec
        trail_min_mfe = PVIEODSpec().trail_min_mfe
        rows = _exec(
            """SELECT symbol, entry_price,
                      metadata->'_fm_eod_log'->-1 AS last_fm
               FROM trades_v2
               WHERE strategy_id = 'PVI_EOD'
                 AND exit_time IS NULL
                 AND jsonb_array_length(COALESCE(metadata->'_fm_eod_log', '[]'::jsonb)) > 0""",
            fetch='all'
        ) or []
        data = {}
        for r in rows:
            fm = r.get('last_fm') or {}
            if not isinstance(fm, dict):
                continue
            data[r['symbol']] = {
                'entry_price':       float(r['entry_price']) if r.get('entry_price') is not None else None,
                'trail_retrace_pct': fm.get('trail_retrace_pct'),
                'take_profit_pct':   fm.get('take_profit_pct'),
                'loss_ceiling_pct':  fm.get('loss_ceiling_pct'),
                'mfe_pct':           fm.get('mfe_pct'),
                'pnl_pct':           fm.get('pnl_pct'),
                'posture':           fm.get('posture'),
                'confidence':        fm.get('confidence'),
                'rationale':         fm.get('rationale'),
            }
        return jsonify({'data': data, 'trail_min_mfe': trail_min_mfe})
    except Exception as e:
        logger.error(f'[pvi_eod/fm-latest] {e}')
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/pvi/entry-shadow-latest', methods=['GET'])
def pvi_entry_shadow_latest():
    """Latest ESC-vs-tranche entry comparison per PVI symbol, per trade_date.
    Pure observation feed — see project_pvi_recovery_short_hypothesis memory.
    Not scoped to "today" — the closed-trades history table needs this for
    whichever day each trade actually happened on, not just the current one
    (a "today"-only filter went stale the instant the calendar rolled over).
    Returns {"symbol|trade_date": {esc_entry_price, tranche_entry_price}}."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import _exec
        rows = _exec(
            """SELECT DISTINCT ON (symbol, trade_date)
                   symbol, trade_date::text AS trade_date,
                   ROUND(esc_entry_price::numeric, 2)     AS esc_entry_price,
                   ROUND(tranche_entry_price::numeric, 2) AS tranche_entry_price
               FROM pvi_entry_tranche_shadow
               ORDER BY symbol, trade_date, logged_at DESC""",
            fetch='all'
        ) or []
        data = {}
        for r in rows:
            for k in ('esc_entry_price', 'tranche_entry_price'):
                if r.get(k) is not None:
                    r[k] = float(r[k])
            data[f"{r['symbol']}|{r['trade_date']}"] = {
                'esc_entry_price': r['esc_entry_price'],
                'tranche_entry_price': r['tranche_entry_price'],
            }
        return jsonify({'data': data})
    except Exception as e:
        logger.error(f'[pvi/entry-shadow-latest] {e}')
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/subscriber/live', methods=['GET'])
def subscriber_live():
    """Open positions with unrealized P&L scaled to subscriber's capital."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401

    role = session.get('sub_role', 'subscriber')
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from tools.capital_tool import get_capital_summary_today

        if role in ('owner', 'observer'):
            from db.queries import _exec
            _live_sids = _live_strategy_ids('live')
            _pholders  = ','.join(['%s'] * len(_live_sids)) if _live_sids else 'NULL'
            raw = _exec(
                f"""SELECT trade_id::text AS id, strategy_id, symbol, zone,
                          CAST(entry_price      AS FLOAT) AS entry_price,
                          CAST(stop_price       AS FLOAT) AS stop_price,
                          CAST(target_price     AS FLOAT) AS target_price,
                          CAST(capital_deployed AS FLOAT) AS capital_deployed,
                          CAST(entry_qty        AS INT)   AS entry_qty,
                          entry_time,
                          agent_approved_by,
                          metadata::text AS metadata
                   FROM trades_v2
                   WHERE exit_time IS NULL
                     AND strategy_id IN ({_pholders})
                   ORDER BY entry_time DESC""",
                tuple(_live_sids), fetch='all'
            ) or []
            # Determine direction and qty; fetch LTPs for unrealized P&L
            ltp_map = {}
            try:
                kite = KiteHelper.get_kite_client()
                if kite and raw:
                    symbols = list({p['symbol'] for p in raw})
                    quotes  = kite.quote([f'NSE:{s}' for s in symbols]) or {}
                    ltp_map = {s: quotes.get(f'NSE:{s}', {}).get('last_price', 0) for s in symbols}
            except Exception:
                pass
            for p in raw:
                direction, qty, ltp, unrl = _position_direction_qty_unrealized(p, ltp_map)
                p['direction']     = direction
                p['qty']           = qty
                p['unrealized_rs'] = unrl
                if p.get('entry_time') and hasattr(p['entry_time'], 'isoformat'):
                    p['entry_time'] = p['entry_time'].isoformat()
                meta_raw = p.get('metadata')
                if isinstance(meta_raw, str):
                    try:
                        p['metadata'] = json.loads(meta_raw)
                    except (ValueError, TypeError):
                        p['metadata'] = {}
                elif not isinstance(meta_raw, dict):
                    p['metadata'] = {}
            positions = raw

            # Peak daily capital today — Kite-margins()-derived (DailyCapitalDB,
            # migration 016), not a SUM of trades_v2.capital_deployed. The old
            # SUM double-counted capital reused across same-day trades (e.g.
            # 2026-07-20's ~35 trades cycling through the same ~4-500k pool
            # summed to a figure in the millions) and this branch's query didn't
            # even filter by strategy, so it summed every strategy's activity
            # together as if concurrent regardless of overlap.
            from datetime import date as _dt_date_live
            from db.queries import DailyCapitalDB
            _snap = DailyCapitalDB.get(_dt_date_live.today())
            peak_cap = float((_snap or {}).get('peak_capital_used') or 0)
            if peak_cap <= 0:
                # No snapshot yet today (pre-market capture / orchestrator
                # sampling not deployed/run yet under the new code) — fall
                # back to the old approximation rather than showing 0.
                _today_start_q = "DATE_TRUNC('day', NOW() AT TIME ZONE 'Asia/Kolkata') AT TIME ZONE 'Asia/Kolkata'"
                _today_row = _exec(
                    f"""SELECT SUM(CAST(capital_deployed AS FLOAT)) AS daily_cap
                        FROM trades_v2
                        WHERE entry_time >= {_today_start_q}""",
                    fetch='all'
                ) or []
                peak_cap = float((_today_row[0].get('daily_cap') if _today_row else None) or 0)
        else:
            peak_cap = 0.0
            from db.queries import SubscriberDB
            positions = SubscriberDB.get_subscriber_live_positions(user_id)
            for p in positions:
                master_rs  = p.get('master_pnl_rs') or 0.0
                master_cap = p.get('master_capital') or 0.0
                sub_cap    = float(p.get('capital_per_trade') or 0)
                if master_cap and master_cap > 0 and sub_cap > 0:
                    pct = master_rs / master_cap
                    p['unrealized_rs'] = round(pct * sub_cap, 2)
                else:
                    p['unrealized_rs'] = None
                for key in ('entry_time',):
                    if p.get(key) and hasattr(p[key], 'isoformat'):
                        p[key] = p[key].isoformat()

        sub_open_count = len(positions)
        master_summary = get_capital_summary_today()
        sub_capital_per_trade = float(positions[0].get('capital_deployed') or 0) if positions else 0

        capital = {
            'sub_current_allocated': sum(float(p.get('capital_deployed') or 0) for p in positions),
            'sub_capital_per_trade': sub_capital_per_trade,
            'sub_open_count':        sub_open_count,
            'master_deployed':       master_summary.get('deployed_today', 0),
            'master_available':      master_summary.get('available', 0),
            'master_total_budget':   master_summary.get('total_budget', 0),
            'peak_daily_capital': round(peak_cap, 0),
        }

        return jsonify({'positions': positions, 'capital': capital})
    except Exception as e:
        logger.error(f"[subscriber/live] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/subscriber/kite-pnl', methods=['GET'])
def subscriber_kite_pnl():
    """Owner-only: P&L with Kite attribution. Today = live Kite API; FY/All = trades_v2 fills."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    role = session.get('sub_role', 'subscriber')
    if role != 'owner':
        return jsonify({'error': 'owner_only'}), 403

    period      = request.args.get('period', 'today')
    strategy_id = request.args.get('strategy') or None
    mode        = request.args.get('mode', 'live')

    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')

        # ── Historical periods: use trades_v2 (Kite fill prices = same gross P&L) ──
        if period != 'today':
            from db.queries import _exec
            from datetime import date as _dt_date
            _LIVE_SIDS = [strategy_id] if strategy_id else _live_strategy_ids(mode)
            fy_year  = _dt_date.today().year if _dt_date.today().month >= 4 else _dt_date.today().year - 1
            fy_start = _dt_date(fy_year, 4, 1)
            pholders = ','.join(['%s'] * len(_LIVE_SIDS)) if _LIVE_SIDS else 'NULL'
            if period == 'fy':
                pclause = "AND trade_date >= %s"
                pparams = [fy_start] + list(_LIVE_SIDS)
            else:
                pclause = ''
                pparams = list(_LIVE_SIDS)

            # "Kite" mode only shows trades Kite actually confirms/executed —
            # exclude NO_FILL rows (orders that never got a real broker fill,
            # entry_price==exit_price, pnl always 0). Total/live mode (subscriber_pnl)
            # intentionally keeps these; this endpoint is the Kite-confirmed subset.
            agg = _exec(
                f"""SELECT strategy_id,
                           COUNT(*) AS n_closed,
                           SUM(CASE WHEN pnl_rupees > 0 THEN 1 ELSE 0 END) AS n_wins,
                           SUM(CAST(pnl_rupees AS FLOAT)) AS gross_pnl_rs
                    FROM trades_v2
                    WHERE exit_time IS NOT NULL {pclause}
                      AND strategy_id IN ({pholders})
                      AND exit_type IS DISTINCT FROM 'NO_FILL'
                    GROUP BY strategy_id""",
                tuple(pparams), fetch='all'
            ) or []

            detail = _exec(
                f"""SELECT symbol, strategy_id, zone,
                           trade_date::text AS trade_date,
                           CAST(entry_price      AS FLOAT) AS entry_price,
                           CAST(exit_price       AS FLOAT) AS exit_price,
                           CAST(pnl_rupees       AS FLOAT) AS gross_pnl,
                           CAST(capital_deployed AS FLOAT) AS capital
                    FROM trades_v2
                    WHERE exit_time IS NOT NULL {pclause}
                      AND strategy_id IN ({pholders})
                      AND exit_type IS DISTINCT FROM 'NO_FILL'
                    ORDER BY exit_time DESC
                    LIMIT 300""",
                tuple(pparams), fetch='all'
            ) or []

            for r in detail:
                zone = r.get('zone', '') or ''
                r['direction']    = 'SHORT' if 'SHORT' in zone.upper() else 'LONG'
                r['is_overnight'] = r.get('strategy_id') == 'EODR'
                r['product']      = 'CNC' if r.get('strategy_id') == 'EODR' else 'MIS'

            strat_rows = [{
                'strategy_id':     r['strategy_id'],
                'n_closed':        int(r['n_closed'] or 0),
                'n_open':          0,
                'n_wins':          int(r['n_wins'] or 0),
                'n_trades':        int(r['n_closed'] or 0),
                'gross_pnl_rs':    round(float(r['gross_pnl_rs'] or 0), 2),
                'open_unrealised': 0.0,
            } for r in agg]

            # Peak capital deployed in-period (same pattern as subscriber_pnl,
            # app2.py ~4891-4912) — supports the frontend's Return on Capital tile.
            _pk_clause  = "AND trade_date >= %s::date" if period == 'fy' else ''
            _pk_params  = list(_LIVE_SIDS) + ([fy_start] if period == 'fy' else [])
            _daily = _exec(
                f"""SELECT trade_date, SUM(CAST(capital_deployed AS FLOAT)) AS daily_cap
                    FROM trades_v2
                    WHERE strategy_id IN ({pholders}) {_pk_clause}
                    GROUP BY trade_date""",
                tuple(_pk_params), fetch='all'
            ) or []
            _peak = max((float(r.get('daily_cap') or 0) for r in _daily), default=0.0)

            totals = {
                'strategy_id':        '__total__',
                'n_trades':           sum(r['n_trades'] for r in strat_rows),
                'n_closed':           sum(r['n_closed'] for r in strat_rows),
                'n_open':             0,
                'n_wins':             sum(r['n_wins']   for r in strat_rows),
                'gross_pnl_rs':       round(sum(r['gross_pnl_rs'] for r in strat_rows), 2),
                'open_unrealised':    0.0,
                'peak_daily_capital': round(_peak, 0),
            }
            return jsonify({'rows': strat_rows, 'totals': totals, 'closed': detail,
                            'open': [], 'period': period, 'source': 'db_fills'})

        # ── Today: live Kite positions API ────────────────────────────────────────
        kite = KiteHelper.get_kite_client()
        if not kite:
            return jsonify({'error': 'kite_unavailable'}), 503
        from db.queries import _exec

        # Our own DB is the source of truth for open-vs-closed (exit_time IS
        # NULL / IS NOT NULL is unambiguous). Kite's positions()/'net' array is
        # NOT a reliable signal for this: selling a CNC holding that was bought
        # on a prior day shows up as a synthetic *negative-quantity* "position"
        # (since only the sell, not the original buy, is part of "today"'s
        # activity) — its 'pnl' field marks that synthetic short's entry price
        # against today's still-moving LTP, which has nothing to do with the
        # real trade's entry-vs-exit economics. Confirmed live: CAMPUS/AFCONS/
        # MIRZAINT all closed today with real losses (-402/-500/-507 per our
        # confirmed fills), while Kite's positions() 'pnl' for the same 3
        # showed +136/-0.90/+97.14 — a different question entirely, not a
        # rounding/charges difference. So: DB decides open vs closed; Kite is
        # consulted only for the live price of positions the DB says are open.
        allowed_sids = {strategy_id} if strategy_id else set(_live_strategy_ids(mode))
        pholders = ','.join(['%s'] * len(allowed_sids)) if allowed_sids else 'NULL'

        open_db_rows = _exec(
            f"SELECT symbol, strategy_id FROM trades_v2 "
            f"WHERE exit_time IS NULL AND strategy_id IN ({pholders})",
            tuple(allowed_sids), fetch='all'
        ) or []

        closed_db_rows = _exec(
            f"""SELECT symbol, strategy_id, zone,
                       CAST(entry_price      AS FLOAT) AS entry_price,
                       CAST(exit_price       AS FLOAT) AS exit_price,
                       CAST(pnl_rupees       AS FLOAT) AS gross_pnl,
                       CAST(capital_deployed AS FLOAT) AS capital
                FROM trades_v2
                WHERE exit_time IS NOT NULL
                  AND exit_time >= DATE_TRUNC('day', NOW() AT TIME ZONE 'Asia/Kolkata') AT TIME ZONE 'Asia/Kolkata'
                  AND strategy_id IN ({pholders})
                  AND exit_type IS DISTINCT FROM 'NO_FILL'""",
            tuple(allowed_sids), fetch='all'
        ) or []

        closed = []
        for r in closed_db_rows:
            zone = r.get('zone', '') or ''
            direction = 'SHORT' if 'SHORT' in zone.upper() else 'LONG'
            closed.append({
                'symbol':       r['symbol'],
                'strategy_id':  r['strategy_id'],
                'product':      'CNC',
                'gross_pnl':    round(r['gross_pnl'] or 0, 2),
                'entry_price':  round(r['entry_price'] or 0, 2),
                'exit_price':   round(r['exit_price'] or 0, 2),
                'direction':    direction,
                'is_overnight': True,
                'capital':      round(r['capital'] or 0, 0),
            })

        # Live price lookup for symbols the DB says are still open — holdings()
        # first (the accurate current remaining quantity + live pnl for a
        # settled CNC position), falling back to positions()/'net' for a fresh
        # same-day entry that hasn't settled into holdings() yet.
        holdings_by_symbol = {h['tradingsymbol']: h for h in (kite.holdings() or [])}
        positions_by_symbol = {p['tradingsymbol']: p for p in kite.positions().get('net', [])}

        open_pos = []
        for r in open_db_rows:
            symbol, strat = r['symbol'], r['strategy_id']
            h = holdings_by_symbol.get(symbol)
            if h and float(h.get('opening_quantity') or 0) > 0:
                qty = float(h['opening_quantity'])
                avg_price = float(h.get('average_price') or 0)
                open_pos.append({
                    'symbol': symbol, 'strategy_id': strat, 'product': h.get('product', 'CNC'),
                    'qty': qty, 'avg_price': round(avg_price, 2),
                    'last_price': round(float(h.get('last_price') or 0), 2),
                    'unrealised': round(float(h.get('pnl') or 0), 2),
                    'capital': round(qty * avg_price, 0), 'direction': 'LONG',
                })
                continue
            p = positions_by_symbol.get(symbol)
            if p and float(p.get('quantity') or 0) > 0:
                qty = float(p['quantity'])
                avg_price = float(p.get('average_price') or 0)
                open_pos.append({
                    'symbol': symbol, 'strategy_id': strat, 'product': p.get('product', ''),
                    'qty': qty, 'avg_price': round(avg_price, 2),
                    'last_price': round(float(p.get('last_price') or 0), 2),
                    'unrealised': round(float(p.get('unrealised') or 0), 2),
                    'capital': round(qty * avg_price, 0), 'direction': 'LONG',
                })
            # else: DB says open but not found as a genuine positive holding/position on
            # Kite right now — a real reconciliation gap, not something to fabricate a number for.

        # Aggregate by strategy
        by_strat: dict = {}
        for pos in closed:
            sid = pos['strategy_id']
            if sid not in by_strat:
                by_strat[sid] = {'strategy_id': sid, 'n_closed': 0, 'n_open': 0,
                                 'n_wins': 0, 'gross_pnl_rs': 0.0, 'open_unrealised': 0.0}
            by_strat[sid]['n_closed'] += 1
            by_strat[sid]['n_wins']   += 1 if pos['gross_pnl'] > 0 else 0
            by_strat[sid]['gross_pnl_rs'] = round(by_strat[sid]['gross_pnl_rs'] + pos['gross_pnl'], 2)

        for pos in open_pos:
            sid = pos['strategy_id']
            if sid not in by_strat:
                by_strat[sid] = {'strategy_id': sid, 'n_closed': 0, 'n_open': 0,
                                 'n_wins': 0, 'gross_pnl_rs': 0.0, 'open_unrealised': 0.0}
            by_strat[sid]['n_open'] += 1
            by_strat[sid]['open_unrealised'] = round(by_strat[sid]['open_unrealised'] + pos['unrealised'], 2)

        rows = list(by_strat.values())
        for r in rows:
            r['n_trades'] = r['n_closed'] + r['n_open']

        closed_gross    = round(sum(p['gross_pnl'] for p in closed), 2)
        open_unrealised = round(sum(p['unrealised'] for p in open_pos), 2)

        # Peak capital deployed today — Kite-margins()-derived when portfolio-wide
        # (DailyCapitalDB, migration 016; see subscriber_pnl for the full
        # rationale — the old SUM-by-trade_date double-counts capital reused
        # across same-day trades and can't see overlap). Falls back to the old
        # approximation when a single strategy is selected (margins() is
        # account-level, can't isolate one strategy) OR when today's snapshot
        # row doesn't exist yet / has no peak recorded (pre-market capture +
        # at least one orchestrator cycle need to have run under the new code).
        _peak = 0.0
        if not strategy_id:
            from datetime import date as _dt_date_kpnl
            from db.queries import DailyCapitalDB
            _snap = DailyCapitalDB.get(_dt_date_kpnl.today())
            _peak = float((_snap or {}).get('peak_capital_used') or 0)
        if strategy_id or _peak <= 0:
            _sids_for_peak = list(allowed_sids)
            _pk_pholders = ','.join(['%s'] * len(_sids_for_peak)) if _sids_for_peak else 'NULL'
            _daily = _exec(
                f"""SELECT trade_date, SUM(CAST(capital_deployed AS FLOAT)) AS daily_cap
                    FROM trades_v2
                    WHERE entry_time >= DATE_TRUNC('day', NOW() AT TIME ZONE 'Asia/Kolkata') AT TIME ZONE 'Asia/Kolkata'
                      AND strategy_id IN ({_pk_pholders})
                    GROUP BY trade_date""",
                tuple(_sids_for_peak), fetch='all'
            ) or []
            _peak = max((float(r.get('daily_cap') or 0) for r in _daily), default=0.0)

        totals = {
            'strategy_id':        '__total__',
            'n_trades':           sum(r['n_trades'] for r in rows),
            'n_closed':           sum(r['n_closed'] for r in rows),
            'n_open':             sum(r['n_open'] for r in rows),
            'n_wins':             sum(r['n_wins'] for r in rows),
            'gross_pnl_rs':       closed_gross,
            'open_unrealised':    open_unrealised,
            'peak_daily_capital': round(_peak, 0),
        }

        return jsonify({
            'rows':            rows,
            'totals':          totals,
            'closed':          sorted(closed,   key=lambda x: -abs(x['gross_pnl'])),
            'open':            sorted(open_pos, key=lambda x: -abs(x['unrealised'])),
            'closed_gross':    closed_gross,
            'open_unrealised': open_unrealised,
            'kite_mode':       True,
            'period':          'today',
        })
    except Exception as e:
        logger.error(f"[subscriber/kite-pnl] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/subscriber/position/<int:sub_pos_id>/exit', methods=['POST'])
def subscriber_exit_position(sub_pos_id):
    """Subscriber-initiated exit: place market order via their broker + mark closed in DB."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    try:
        conn = get_db_connection()
        cur  = conn.cursor()
        # Verify ownership and fetch position details
        cur.execute("""
            SELECT st.symbol, st.direction, st.qty, st.strategy_id,
                   ubs.broker_type, ubs.session_token, ubs.api_key
            FROM subscriber_trades st
            JOIN user_strategy_subscriptions uss
                ON uss.user_id = %s AND uss.strategy_id = st.strategy_id
            LEFT JOIN user_broker_sessions ubs ON ubs.user_id = %s
            WHERE st.id = %s AND st.status = 'open' AND st.user_id = %s
        """, (user_id, user_id, sub_pos_id, user_id))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return jsonify({'error': 'position_not_found'}), 404
        symbol, direction, qty, strategy_id, broker_type, session_token, api_key = row
        qty = int(qty or 0)
        order_id = None
        if broker_type == 'kite' and session_token and qty > 0:
            try:
                from kiteconnect import KiteConnect
                kite = KiteConnect(api_key=api_key or app.config.get('KITE_API_KEY', ''))
                kite.set_access_token(session_token)
                opp = 'SELL' if (direction or '').upper() == 'LONG' else 'BUY'
                order_id = kite.place_order(
                    variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=symbol, transaction_type=opp, quantity=qty,
                    product=kite.PRODUCT_MIS, order_type=kite.ORDER_TYPE_MARKET
                )
            except Exception as oe:
                logger.error(f"[subscriber/exit/kite] {oe}")
                cur.close(); conn.close()
                return jsonify({'error': f'broker_error: {oe}'}), 500
        cur.execute(
            "UPDATE subscriber_trades SET status='closed', exit_time=NOW(), exit_type='MANUAL' WHERE id=%s",
            (sub_pos_id,)
        )
        conn.commit()
        cur.close(); conn.close()
        return jsonify({'ok': True, 'order_id': order_id})
    except Exception as e:
        logger.error(f"[subscriber/exit] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/subscriber/position/<int:sub_pos_id>/double', methods=['POST'])
def subscriber_double_position(sub_pos_id):
    """Subscriber-initiated 2x: place another market order same direction + double qty in DB."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    try:
        conn = get_db_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT st.symbol, st.direction, st.qty, st.strategy_id,
                   ubs.broker_type, ubs.session_token, ubs.api_key
            FROM subscriber_trades st
            JOIN user_strategy_subscriptions uss
                ON uss.user_id = %s AND uss.strategy_id = st.strategy_id
            LEFT JOIN user_broker_sessions ubs ON ubs.user_id = %s
            WHERE st.id = %s AND st.status = 'open' AND st.user_id = %s
        """, (user_id, user_id, sub_pos_id, user_id))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return jsonify({'error': 'position_not_found'}), 404
        symbol, direction, qty, strategy_id, broker_type, session_token, api_key = row
        qty = int(qty or 0)
        order_id = None
        if broker_type == 'kite' and session_token and qty > 0:
            try:
                from kiteconnect import KiteConnect
                kite = KiteConnect(api_key=api_key or app.config.get('KITE_API_KEY', ''))
                kite.set_access_token(session_token)
                txn = 'BUY' if (direction or '').upper() == 'LONG' else 'SELL'
                order_id = kite.place_order(
                    variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=symbol, transaction_type=txn, quantity=qty,
                    product=kite.PRODUCT_MIS, order_type=kite.ORDER_TYPE_MARKET
                )
            except Exception as oe:
                logger.error(f"[subscriber/double/kite] {oe}")
                cur.close(); conn.close()
                return jsonify({'error': f'broker_error: {oe}'}), 500
        cur.execute("UPDATE subscriber_trades SET qty = qty * 2 WHERE id = %s", (sub_pos_id,))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({'ok': True, 'order_id': order_id})
    except Exception as e:
        logger.error(f"[subscriber/double] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/subscriber/investment', methods=['POST'])
def subscriber_update_investment():
    """Update investment amount for a strategy. Scales P&L proportionally."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401

    body = request.get_json(force=True) or {}
    strategy_id = body.get('strategy_id')
    investment_amount = body.get('investment_amount')  # in rupees
    max_capital = body.get('max_capital')  # optional, rupees — store-only, not yet enforced
    enabled = body.get('enabled', True)    # allows subscribing to a new strategy from this endpoint

    if not strategy_id or investment_amount is None:
        return jsonify({'error': 'missing_params'}), 400

    try:
        investment_amount = float(investment_amount)
        if investment_amount < 0:
            return jsonify({'error': 'invalid_amount'}), 400
        if max_capital is not None:
            max_capital = int(max_capital)
            if max_capital < investment_amount:
                return jsonify({'error': 'max_capital_below_investment'}), 400

        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import SubscriberDB

        # Get base capital for this strategy from config/spec
        base_capital_map = {
            'BLOCK_DEAL_FADE': 200_000,  # ₹2L base capital
            'PVI': 200_000,
            'HA_REVERSAL': 200_000,
            'ZONE_S21': 150_000,
        }
        base_capital = base_capital_map.get(strategy_id.upper(), 200_000)

        # Update subscription with new capital_per_trade (+ optional max_capital)
        SubscriberDB.upsert_strategy_subscription(
            user_id, strategy_id, int(investment_amount),
            enabled=bool(enabled), max_capital=max_capital,
        )

        return jsonify({
            'ok': True,
            'strategy_id': strategy_id,
            'investment_amount': investment_amount,
            'max_capital': max_capital,
            'base_capital': base_capital,
            'scaling_factor': investment_amount / base_capital if base_capital > 0 else 1.0,
        })
    except Exception as e:
        logger.error(f"[subscriber/investment] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/subscriber/logout', methods=['GET'])
def subscriber_logout():
    session.pop('sub_user_id', None)
    session.pop('sub_name', None)
    session.pop('sub_kite_id', None)
    session.pop('sub_role', None)
    # /subscribe is the one front door for every role now (including the
    # inline observer email/OTP flow) — send everyone back there on sign-out,
    # not just subscribers, so it's the same page regardless of how they got in.
    return redirect('/subscribe')


@app.route('/api/subscriber/equity-curve', methods=['GET'])
def subscriber_equity_curve():
    """Daily cumulative net P&L series for chart rendering."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401

    strategy_id = request.args.get('strategy') or None
    mode        = request.args.get('mode', 'live')
    period      = request.args.get('period', 'all')     # today | fy | all
    source      = request.args.get('source', 'total')    # total | kite
    role        = session.get('sub_role', 'subscriber')

    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from trading_costs import compute_charges

        if role in ('owner', 'observer'):
            from db.queries import _exec
            _LIVE_SIDS = _live_strategy_ids(mode)
            if strategy_id:
                sid_clause = 'AND strategy_id = %s'
                sid_params = [strategy_id]
            elif _LIVE_SIDS:
                placeholders = ','.join(['%s'] * len(_LIVE_SIDS))
                sid_clause = f'AND strategy_id IN ({placeholders})'
                sid_params = list(_LIVE_SIDS)
            else:
                sid_clause = 'AND FALSE'
                sid_params = []

            # period: same date-window convention used by subscriber_pnl/kite-pnl
            period_clause = ''
            period_params = []
            if period == 'today':
                period_clause = ("AND exit_time >= DATE_TRUNC('day', NOW() AT TIME ZONE 'Asia/Kolkata')"
                                  " AT TIME ZONE 'Asia/Kolkata'")
            elif period == 'fy':
                from datetime import date as _dt_date
                fy_year  = _dt_date.today().year if _dt_date.today().month >= 4 else _dt_date.today().year - 1
                fy_start = _dt_date(fy_year, 4, 1)
                period_clause = "AND exit_time >= %s::date"
                period_params = [fy_start]

            # source='kite': only trades Kite actually confirms/executed —
            # excludes NO_FILL rows (orders that never got a real broker fill).
            source_clause = "AND exit_type IS DISTINCT FROM 'NO_FILL'" if source == 'kite' else ''

            raw = _exec(
                f"""SELECT (exit_time AT TIME ZONE 'Asia/Kolkata')::date AS trade_date,
                           strategy_id, product,
                           CAST(pnl_rupees       AS FLOAT) AS pnl_rupees,
                           CAST(capital_deployed AS FLOAT) AS capital_deployed,
                           (entry_time AT TIME ZONE 'Asia/Kolkata')::date
                               != (exit_time AT TIME ZONE 'Asia/Kolkata')::date AS held_overnight
                    FROM trades_v2
                    WHERE exit_time IS NOT NULL {sid_clause} {period_clause} {source_clause}
                    ORDER BY trade_date""",
                tuple(sid_params + period_params), fetch='all'
            ) or []
        else:
            from db.queries import SubscriberDB
            raw = SubscriberDB.get_subscriber_equity_curve_data(user_id, strategy_id)

        daily: dict = {}
        for t in raw:
            if role in ('owner', 'observer'):
                gross = float(t['pnl_rupees'] or 0)
                cap   = float(t['capital_deployed'] or 0)
                from trading_costs import charges_for_trade as _cft
                net   = gross - (_cft(cap, t.get('strategy_id', ''), product=t.get('product'),
                                       held_overnight=t.get('held_overnight', True)) if cap > 0 else 0.0)
            else:
                ep  = float(t['entry_price'] or 0)
                xp  = float(t['exit_price']  or 0)
                qty = float(t['qty']         or 0)
                cap = float(t.get('capital_deployed') or ep * qty)
                gross = (xp - ep) * qty if t.get('direction') == 'LONG' else (ep - xp) * qty
                net   = gross - (compute_charges(cap)['total'] if cap > 0 else 0.0)
            d = str(t['trade_date'])
            daily[d] = round(daily.get(d, 0.0) + net, 2)

        cumulative = 0.0
        series = []
        for d in sorted(daily):
            cumulative = round(cumulative + daily[d], 2)
            series.append({'date': d, 'daily_net_pnl': daily[d], 'cumulative_net_pnl': cumulative})

        return jsonify({'series': series})
    except Exception as e:
        logger.error(f"[subscriber/equity-curve] {e}")
        return jsonify({'error': 'server_error'}), 500


# =============================================================================
# OWNER / OBSERVER ROLES
# =============================================================================

def _sub_session_role():
    """Return role from session: 'owner' | 'subscriber' | 'observer' | None."""
    return session.get('sub_role')


def _send_otp_email(to_email: str, otp: str) -> bool:
    """Send OTP via SMTP. Returns True on success, False if SMTP not configured."""
    import smtplib as _smtplib
    from email.mime.text import MIMEText as _MIMEText
    smtp_host  = os.getenv('OTP_SMTP_HOST', 'smtp.gmail.com')
    smtp_port  = int(os.getenv('OTP_SMTP_PORT', '587'))
    smtp_user  = os.getenv('OTP_SMTP_USER', '')
    smtp_pass  = os.getenv('OTP_SMTP_PASS', '')
    from_email = os.getenv('OTP_FROM_EMAIL', smtp_user)
    if not smtp_user or not smtp_pass:
        logger.warning(f"[otp] SMTP not configured — OTP for {to_email}: {otp}")
        return False
    try:
        msg = _MIMEText(
            f"Your Alaidin observer login code is: {otp}\n\nThis code expires in 10 minutes."
        )
        msg['Subject'] = f'Alaidin login code: {otp}'
        msg['From'] = from_email
        msg['To'] = to_email
        with _smtplib.SMTP(smtp_host, smtp_port) as srv:
            srv.starttls()
            srv.login(smtp_user, smtp_pass)
            srv.sendmail(from_email, [to_email], msg.as_string())
        return True
    except Exception as e:
        logger.error(f"[otp] SMTP send failed to {to_email}: {e}")
        return False


def _kite_owner_callback():
    """Handle Kite OAuth for owner. Called from kite_callback_fixed when state=owner."""
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from config import OWNER_KITE_ID

        request_token = request.args.get('request_token')
        if not request_token:
            return redirect('/login?error=missing_token')

        kite = KiteConnect(api_key=app.config['KITE_API_KEY'])
        session_data = kite.generate_session(
            request_token=request_token,
            api_secret=app.config['KITE_API_SECRET'],
        )
        access_token = session_data['access_token']
        kite_user_id = session_data.get('user_id', '')

        if kite_user_id != OWNER_KITE_ID:
            logger.warning(f"[owner-callback] Rejected non-owner: {kite_user_id}")
            return redirect('/login?error=not_owner')

        kite.set_access_token(access_token)
        profile_data = kite.profile()
        dmat_name = profile_data.get('user_name', kite_user_id)

        import psycopg2 as _pg_own
        conn = _pg_own.connect(
            host=app.config['DB_HOST'], database=app.config['DB_NAME'],
            user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE username = %s", (kite_user_id,))
            row = cur.fetchone()
            if row:
                user_id = row[0]
                cur.execute("UPDATE users SET role = 'owner' WHERE id = %s", (user_id,))
            else:
                import hashlib as _hl, secrets as _sc
                pw_hash = _hl.sha256(_sc.token_bytes(32)).hexdigest()
                cur.execute(
                    "INSERT INTO users (username, password, role) VALUES (%s, %s, 'owner') RETURNING id",
                    (kite_user_id, pw_hash),
                )
                user_id = cur.fetchone()[0]

            # user_broker_sessions: active immediately, no approval needed
            cur.execute(
                """
                INSERT INTO user_broker_sessions
                    (user_id, broker_type, api_key, session_token, expires_at,
                     is_active, pending_approval)
                VALUES (%s, 'kite', %s, %s, NOW() + INTERVAL '1 day', TRUE, FALSE)
                ON CONFLICT (user_id, broker_type) DO UPDATE SET
                    session_token    = EXCLUDED.session_token,
                    api_key          = EXCLUDED.api_key,
                    expires_at       = EXCLUDED.expires_at,
                    is_active        = TRUE,
                    pending_approval = FALSE,
                    created_at       = NOW()
                """,
                (user_id, app.config['KITE_API_KEY'], access_token),
            )
            # api_tokens: MDS reads from here (SELECT token FROM api_tokens WHERE broker_type='kite' ...)
            cur.execute(
                "INSERT INTO api_tokens (token, broker_type, created_at) VALUES (%s, 'kite', NOW())",
                (access_token,),
            )

        conn.commit()
        conn.close()

        logger.info(f"[owner-callback] Owner login: {dmat_name} ({kite_user_id}) uid={user_id}")
        # Bypass double-redirect issue: callback is on 143.244.142.66, dashboard is on
        # alaidin.info. A server-side redirect from the callback loses the ?t= param via
        # the nginx 301 that canonicalises the domain. Use a JS page to navigate directly.
        import secrets as _sec
        dash_token = _sec.token_hex(16)
        _dashboard_token_write(dash_token, user_id, dmat_name, kite_user_id, 'owner')
        dest = f'https://alaidin.info/api/subscribe/enter-dashboard?token={dash_token}'
        return f'''<!doctype html><html><head><meta charset=utf-8><title>Logging in…</title>
<meta http-equiv="refresh" content="0;url={dest}">
</head><body><script>window.location.replace('{dest}');</script>
<p>Completing login…</p></body></html>'''

    except Exception as e:
        logger.error(f"[owner-callback] error: {e}")
        return redirect(f'/login?error={str(e)[:60]}')


@app.route('/observer', methods=['GET'])
def observer_page():
    return send_from_directory(app.static_folder, 'observer.html')


@app.route('/api/auth/kite/owner-login', methods=['GET'])
@app.route('/kite-login', methods=['GET'])
def kite_owner_login():
    """Full-page redirect to Kite OAuth with state=owner.

    /kite-login is a short bookmark-friendly alias for mobile Safari.
    Always uses state=owner → _kite_owner_callback() → clean api_tokens write.
    """
    api_key = app.config.get('KITE_API_KEY', '')
    if not api_key:
        return "Kite API key not configured", 500
    return redirect(
        f"https://kite.zerodha.com/connect/login?api_key={api_key}&v=3&state=owner"
    )


@app.route('/api/observer/request-otp', methods=['POST'])
def observer_request_otp():
    """Generate and email a 6-digit OTP to the provided address."""
    import re, bcrypt, secrets as _otp_sec
    body  = request.get_json(force=True) or {}
    email = (body.get('email') or '').strip().lower()
    if not email or not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
        return jsonify({'error': 'Invalid email address.'}), 400

    otp      = str(_otp_sec.randbelow(900000) + 100000)
    otp_hash = bcrypt.hashpw(otp.encode(), bcrypt.gensalt()).decode()
    expires  = datetime.utcnow() + timedelta(minutes=10)

    try:
        import psycopg2 as _pg_otp
        conn = _pg_otp.connect(
            host=app.config['DB_HOST'], database=app.config['DB_NAME'],
            user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO observer_otps (email, otp_hash, expires_at) VALUES (%s, %s, %s)",
                (email, otp_hash, expires),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[observer/request-otp] DB error: {e}")
        return jsonify({'error': 'Server error.'}), 500

    sent = _send_otp_email(email, otp)
    if not sent:
        if os.getenv('OTP_SMTP_USER'):
            return jsonify({'error': 'Failed to send email. Try again.'}), 500
        return jsonify({'status': 'sent', 'dev_otp': otp})  # dev mode

    return jsonify({'status': 'sent'})


@app.route('/api/observer/verify-otp', methods=['POST'])
def observer_verify_otp():
    """Verify OTP and establish an observer session."""
    import bcrypt
    body  = request.get_json(force=True) or {}
    email = (body.get('email') or '').strip().lower()
    otp   = (body.get('otp') or '').strip()
    if not email or not otp:
        return jsonify({'error': 'Email and OTP are required.'}), 400

    try:
        import psycopg2 as _pg_vf
        conn = _pg_vf.connect(
            host=app.config['DB_HOST'], database=app.config['DB_NAME'],
            user=app.config['DB_USER'], password=app.config['DB_PASSWORD'],
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, otp_hash FROM observer_otps
                WHERE email = %s AND used = FALSE AND expires_at > NOW()
                ORDER BY created_at DESC LIMIT 1
                """,
                (email,),
            )
            row = cur.fetchone()

        if not row or not bcrypt.checkpw(otp.encode(), row[1].encode()):
            conn.close()
            return jsonify({'error': 'Invalid or expired code.'}), 400

        otp_id = row[0]
        with conn.cursor() as cur:
            cur.execute("UPDATE observer_otps SET used = TRUE WHERE id = %s", (otp_id,))
            cur.execute("SELECT id FROM users WHERE email = %s", (email,))
            urow = cur.fetchone()
            if urow:
                user_id = urow[0]
                cur.execute("UPDATE users SET email_verified = TRUE WHERE id = %s", (user_id,))
            else:
                import hashlib as _hl2, secrets as _sc2
                pw_hash = _hl2.sha256(_sc2.token_bytes(32)).hexdigest()
                cur.execute(
                    """
                    INSERT INTO users (username, password, role, email, email_verified)
                    VALUES (%s, %s, 'observer', %s, TRUE) RETURNING id
                    """,
                    (email, pw_hash, email),
                )
                user_id = cur.fetchone()[0]

        conn.commit()
        conn.close()

        session['sub_user_id'] = user_id
        session['sub_name']    = email
        session['sub_role']    = 'observer'
        session.permanent      = True

        logger.info(f"[observer/verify-otp] Observer login: {email} uid={user_id}")
        return jsonify({'status': 'ok'})

    except Exception as e:
        logger.error(f"[observer/verify-otp] error: {e}")
        return jsonify({'error': 'Server error.'}), 500


@app.route('/api/observe/strategies', methods=['GET'])
def observe_strategies():
    """Master P&L for all strategies — all logged-in roles. Sorted: live → shadow → disabled."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    # period-scoped, net-of-charges figures for the strategy tiles — added so
    # tiles can match whatever Today/This FY/All Time + net basis the P&L card
    # above them is showing (2026-07-22: tiles vs P&L tab mismatch diagnosis).
    period = request.args.get('period', 'today')
    if period not in ('today', 'fy', 'all'):
        period = 'today'
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import SubscriberDB, StrategyControlsDB
        from config import STRATEGY_CONFIG
        role = session.get('sub_role', 'subscriber')
        is_preview = _show_master_preview(user_id, role)
        rows = (SubscriberDB.get_all_strategies_pnl_normalized()
                if is_preview else SubscriberDB.get_all_strategies_pnl())
        db_map = {r['strategy_id']: r for r in rows}
        period_map = SubscriberDB.get_period_pnl_by_strategy(period, normalized=is_preview)
        # Runtime live/shadow state — DB overrides static config
        controls = {c['strategy_id']: c for c in StrategyControlsDB.get_all()}
        result = []
        for sid, cfg in STRATEGY_CONFIG.items():
            enabled = cfg.get('enabled', False)
            ctrl    = controls.get(sid, {})
            if enabled:
                live = ctrl.get('live_enabled', cfg.get('live_enabled', False))
                mode = 'live' if live else 'shadow'
            else:
                mode = 'disabled'
            db   = db_map.get(sid, {})
            pd_  = period_map.get(sid, {})
            n    = int(db.get('n_trades') or 0)
            wins = int(db.get('n_wins') or 0)
            result.append({
                'strategy_id':    sid,
                'mode':           mode,
                'n_trades':       n,
                'n_trades_today': int(db.get('n_trades_today') or 0),
                'win_rate':       round(wins / n, 4) if n else 0,
                'avg_trade_rs':   round(float(db.get('avg_trade_rs') or 0), 2),
                'total_pnl_rs':   round(float(db.get('total_pnl_rs') or 0), 2),
                'today_pnl_rs':   round(float(db.get('today_pnl_rs') or 0), 2),
                'open_positions': int(db.get('open_positions') or 0),
                # Period-scoped (matches P&L tab's Today/FY/All Time selector), net of charges
                'period':              period,
                'period_gross_pnl_rs': round(float(pd_.get('gross_pnl_rs') or 0), 2),
                'period_charges_rs':   round(float(pd_.get('charges_rs') or 0), 2),
                'period_net_pnl_rs':   round(float(pd_.get('net_pnl_rs') or 0), 2),
                'period_n_closed':     int(pd_.get('n_closed') or 0),
            })
        # Sort: live first, shadow second, disabled last; within each group open positions first
        _order = {'live': 0, 'shadow': 1, 'disabled': 2}
        result.sort(key=lambda s: (_order.get(s['mode'], 3), -s['open_positions']))
        return jsonify({'strategies': result})
    except Exception as e:
        logger.error(f"[observe/strategies] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/observe/live', methods=['GET'])
def observe_live():
    """All open master-account positions with direction + LTP best-effort — all logged-in roles."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import SubscriberDB
        positions = SubscriberDB.get_all_open_master_positions()

        # Best-effort Kite LTPs
        ltp_map = {}
        try:
            kite = KiteHelper.get_kite_client()
            if kite and positions:
                symbols = list({p['symbol'] for p in positions})
                quotes  = kite.quote([f'NSE:{s}' for s in symbols]) or {}
                ltp_map = {s: quotes.get(f'NSE:{s}', {}).get('last_price', 0) for s in symbols}
        except Exception:
            pass

        result = []
        for p in positions:
            if p.get('entry_time') and hasattr(p['entry_time'], 'isoformat'):
                p['entry_time'] = p['entry_time'].isoformat()
            meta_raw = p.get('metadata')
            if isinstance(meta_raw, str):
                try:
                    p['metadata'] = json.loads(meta_raw)
                except (ValueError, TypeError):
                    p['metadata'] = {}
            elif not isinstance(meta_raw, dict):
                p['metadata'] = {}
            direction, qty, ltp, unrl = _position_direction_qty_unrealized(p, ltp_map)
            result.append({**p, 'direction': direction, 'qty': qty,
                           'current_price': ltp, 'unrealized_rs': unrl})
        return jsonify({'positions': result})
    except Exception as e:
        logger.error(f"[observe/live] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/observe/closed', methods=['GET'])
def observe_closed():
    """Today's closed master-account trades — all logged-in roles."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import SubscriberDB
        trades = SubscriberDB.get_today_closed()
        for t in trades:
            for key in ('entry_time', 'exit_time'):
                if t.get(key) and hasattr(t[key], 'isoformat'):
                    t[key] = t[key].isoformat()
            zone = t.get('zone', '')
            t['direction'] = 'SHORT' if 'SHORT' in zone.upper() else 'LONG'
        return jsonify({'trades': trades})
    except Exception as e:
        logger.error(f"[observe/closed] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/observe/nifty-intraday', methods=['GET'])
def observe_nifty_intraday():
    """Today's NIFTY 50 5-min candles as % change from open, up to current IST time."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import _exec
        rows = _exec(
            """SELECT timestamp, CAST(close AS FLOAT) AS close, CAST(open AS FLOAT) AS open
               FROM intraday_candles_5min
               WHERE symbol = 'NIFTY 50'
                 AND timestamp >= CURRENT_DATE
                 AND timestamp <  CURRENT_DATE + INTERVAL '1 day'
               ORDER BY timestamp""",
            fetch='all'
        ) or []
        if not rows:
            return jsonify({'candles': []})

        # Detect UTC vs IST-naive: if first candle hour < 5 it's UTC
        is_utc = rows[0]['timestamp'].hour < 5
        ist_offset = 5 * 60 + 30  # minutes

        def to_ist_hhmm(ts):
            mins = ts.hour * 60 + ts.minute
            if is_utc:
                mins += ist_offset
            h, m = divmod(mins % (24 * 60), 60)
            return f'{h:02d}:{m:02d}'

        # Only keep candles from 09:15 IST (market open) onwards
        candles = [{'time': to_ist_hhmm(r['timestamp']), 'close': r['close'], 'open': r['open']}
                   for r in rows if to_ist_hhmm(r['timestamp']) >= '09:15']
        if not candles:
            return jsonify({'candles': []})

        # True session open = OPEN of the 09:15 candle (the 09:15:00 print), matching
        # Kite's own ohlc.open and orchestrator._fetch_nifty_open_pct(). Using this
        # candle's CLOSE instead (as before) is really the ~09:20 price and understates
        # %-from-open by however much NIFTY moved in that first 5 minutes.
        open_price = candles[0]['open']
        result = [{'time': c['time'],
                   'pct': round((c['close'] - open_price) / open_price * 100, 3)}
                  for c in candles]
        return jsonify({'candles': result})
    except Exception as e:
        logger.error(f"[observe/nifty-intraday] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/observe/pvi-drift-profile', methods=['GET'])
def observe_pvi_drift_profile():
    """PVI NIFTY drift profile — WR/P per drift bucket from trades_v2.metadata.
    Returns rolling window of trades that have nifty_drift recorded (post May-21-2026).
    Query param: days (default 60)."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    try:
        days = min(int(request.args.get('days', 60)), 365)
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import _exec
        rows = _exec(
            """
            SELECT
                (metadata->>'nifty_drift')::float  AS drift,
                (metadata->>'nifty_max_up')::float  AS max_up,
                CAST(pnl_pct AS FLOAT)              AS pnl_pct,
                exit_type
            FROM trades_v2
            WHERE strategy_id = 'PVI'
              AND exit_time IS NOT NULL
              AND metadata ? 'nifty_drift'
              AND trade_date >= CURRENT_DATE - (%s || ' days')::interval
            """,
            (str(days),),
            fetch='all'
        ) or []

        DRIFT_BUCKETS = [
            ('falling',  None,  -0.30),
            ('neutral',  -0.30,  0.30),
            ('mild-up',   0.30,  0.55),
            ('str-up',    0.55,  None),
        ]
        MAX_UP_BUCKETS = [
            ('low',    None, 0.45),
            ('mid',    0.45, 0.90),
            ('high',   0.90, None),
        ]

        def bucket_stats(trades):
            n = len(trades)
            if n == 0:
                return {'n': 0, 'wr': None, 'p_per_tr': None}
            wins = sum(1 for t in trades if t['exit_type'] not in ('stop', 'force_exit', 'STOP', 'FORCE_EXIT'))
            p = sum(t['pnl_pct'] for t in trades) / n
            return {'n': n, 'wr': round(wins / n * 100, 1), 'p_per_tr': round(p, 3)}

        drift_rows = []
        for label, lo, hi in DRIFT_BUCKETS:
            subset = [r for r in rows
                      if (lo is None or r['drift'] >= lo)
                      and (hi is None or r['drift'] < hi)]
            drift_rows.append({'bucket': label, **bucket_stats(subset)})

        max_up_rows = []
        for label, lo, hi in MAX_UP_BUCKETS:
            subset = [r for r in rows
                      if (lo is None or r['max_up'] >= lo)
                      and (hi is None or r['max_up'] < hi)]
            max_up_rows.append({'bucket': label, **bucket_stats(subset)})

        return jsonify({
            'total_trades': len(rows),
            'days': days,
            'drift_profile': drift_rows,
            'max_up_profile': max_up_rows,
            'data_since': '2026-05-21',
        })
    except Exception as e:
        logger.error(f"[observe/pvi-drift-profile] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/observe/smoke', methods=['GET'])
def observe_smoke():
    """Return last smoke-test results + running status. Auth: any logged-in role."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    import json, os as _os
    pid_path     = '/tmp/smoke_running.pid'
    results_path = '/tmp/smoke_results.json'
    running = False
    if _os.path.exists(pid_path):
        try:
            pid = int(open(pid_path).read().strip())
            _os.kill(pid, 0)   # signal 0 = check existence only
            running = True
        except (ProcessLookupError, ValueError, OSError):
            try: _os.unlink(pid_path)
            except Exception: pass
    if running:
        return jsonify({'status': 'running'})
    if not _os.path.exists(results_path):
        return jsonify({'status': 'never_run'})
    try:
        with open(results_path) as f:
            data = json.load(f)
        data['status'] = 'done'
        return jsonify(data)
    except Exception as e:
        logger.error(f"[observe/smoke] read error: {e}")
        return jsonify({'status': 'never_run'})


@app.route('/api/observe/smoke/run', methods=['POST'])
def observe_smoke_run():
    """Kick off a fresh smoke run in the background. Owner or observer only."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    role = session.get('sub_role', 'subscriber')
    if role not in ('owner', 'observer'):
        return jsonify({'error': 'forbidden'}), 403
    import os as _os, subprocess
    pid_path = '/tmp/smoke_running.pid'
    if _os.path.exists(pid_path):
        try:
            pid = int(open(pid_path).read().strip())
            _os.kill(pid, 0)
            return jsonify({'status': 'already_running'})
        except (ProcessLookupError, ValueError, OSError):
            try: _os.unlink(pid_path)
            except Exception: pass
    try:
        proc = subprocess.Popen(
            ['python3', '/opt/stockapp/tradingv2/pre_session_check.py'],
            cwd='/opt/stockapp',
            env={**_os.environ, 'PYTHONPATH': '/opt/stockapp'},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        logger.info(f"[observe/smoke/run] Launched pre_session_check.py pid={proc.pid}")
        return jsonify({'status': 'started', 'pid': proc.pid})
    except Exception as e:
        logger.error(f"[observe/smoke/run] launch failed: {e}")
        return jsonify({'error': str(e)}), 500

# =============================================================================
# ADMIN API — owner-only endpoints for strategy control + position management
# =============================================================================

def _admin_auth():
    """Return (user_id, None) if caller is owner; (None, error_response) otherwise."""
    user_id, _ = _sub_session_user()
    if not user_id:
        return None, (jsonify({'error': 'not_logged_in'}), 401)
    if session.get('sub_role') != 'owner':
        return None, (jsonify({'error': 'forbidden'}), 403)
    return user_id, None


_PROMPTS_DIR = '/opt/stockapp/tradingv2/agents/prompts'
_AGENTS_DIR  = '/opt/stockapp/tradingv2/agents'

# Agents whose prompt file is versioned via a module-level constant (e.g.
# `_FM_PROMPT_VERSION = 'v7'`) resolve their filename dynamically, read straight
# from the source file on every request — so this list can never drift out of
# sync with whatever version is actually live, the way the old hardcoded
# 'pvi_flow_manager_prompt_v4noex.txt' entry did after v7 shipped.
# Agents with no version constant in code (BRL) keep a static 'file' entry.
_STRATEGY_AGENTS = {
    'PVI': [
        {'id': 'flow_manager', 'label': 'Flow Manager',     'module': 'pvi_flow_manager.py',   'const': '_FM_PROMPT_VERSION',    'tpl': 'pvi_flow_manager_prompt_{}.txt'},
        {'id': 'entry_v2',     'label': 'Entry Agent (V2)',  'module': 'pvi_entry_agent_v2.py', 'const': '_ENTRY_PROMPT_VERSION', 'tpl': 'pvi_entry_v2_prompt_{}.txt'},
        {'id': 't0',           'label': 'T0 Agent',          'module': 'pvi_t0_agent.py',       'const': '_T0_PROMPT_VERSION',    'tpl': 'pvi_t0_prompt_{}.txt'},
    ],
    'PVI_EOD': [
        {'id': 'eod_fm', 'label': 'EOD Flow Manager', 'module': 'pvi_eod_flow_manager.py', 'const': '_FM_EOD_PROMPT_VERSION', 'tpl': 'pvi_eod_flow_manager_prompt_{}.txt'},
    ],
    'BRL': [
        {'id': 'entry',       'label': 'Entry Agent',      'file': 'brl_entry_agent_prompt_v1.txt'},
        {'id': 'pos_manager', 'label': 'Position Manager', 'file': 'brl_position_manager_prompt_v2.txt'},
    ],
}


def _resolve_agent_file(agent_cfg):
    """Return the live prompt filename for an agent entry. Static 'file' entries
    (no version constant in code) pass through unchanged. 'module'/'const'/'tpl'
    entries re-read the module's source on every call and regex out the current
    value of the version constant, so a code-side version bump (e.g. v7 -> v8)
    is reflected here immediately with no admin-console change required."""
    if 'file' in agent_cfg:
        return agent_cfg['file']
    import re
    path = os.path.join(_AGENTS_DIR, agent_cfg['module'])
    try:
        with open(path) as f:
            text = f.read()
    except FileNotFoundError:
        return None
    m = re.search(rf"^{re.escape(agent_cfg['const'])}\s*=\s*['\"]([^'\"]+)['\"]", text, re.M)
    if not m:
        return None
    return agent_cfg['tpl'].format(m.group(1))


def _all_allowed_prompt_files():
    """Recompute the whitelist fresh each call — must track _resolve_agent_file's
    dynamic resolution, or a version bump would make GET/POST reject the new
    (correct) filename because it wasn't in a stale, import-time-frozen set."""
    files = set()
    for agents in _STRATEGY_AGENTS.values():
        for a in agents:
            f = _resolve_agent_file(a)
            if f:
                files.add(f)
    return files


@app.route('/api/admin/strategy/<sid>/agents', methods=['GET'])
def admin_strategy_agents(sid):
    """List agents (with live-resolved prompt file names) for a strategy."""
    uid, err = _admin_auth()
    if err: return err
    agents = _STRATEGY_AGENTS.get(sid.upper(), [])
    resolved = []
    for a in agents:
        f = _resolve_agent_file(a)
        resolved.append({'id': a['id'], 'label': a['label'], 'file': f or '(unresolved)'})
    return jsonify({'agents': resolved})


@app.route('/api/admin/agent-prompt', methods=['GET'])
def admin_agent_prompt_get():
    """Read a prompt file by name (whitelisted)."""
    uid, err = _admin_auth()
    if err: return err
    filename = request.args.get('file', '').strip()
    if filename not in _all_allowed_prompt_files():
        return jsonify({'error': 'not_allowed'}), 403
    path = os.path.join(_PROMPTS_DIR, filename)
    try:
        with open(path) as f:
            return jsonify({'content': f.read(), 'file': filename})
    except FileNotFoundError:
        return jsonify({'error': 'not_found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/agent-prompt', methods=['POST'])
def admin_agent_prompt_save():
    """Save (overwrite) a prompt file. Auto-creates a dated .bak before writing."""
    uid, err = _admin_auth()
    if err: return err
    data     = request.get_json() or {}
    filename = (data.get('file') or '').strip()
    content  = data.get('content', '')
    if filename not in _all_allowed_prompt_files():
        return jsonify({'error': 'not_allowed'}), 403
    path = os.path.join(_PROMPTS_DIR, filename)
    try:
        import shutil
        from datetime import date as _date
        bak = path + '.bak.' + _date.today().strftime('%Y%m%d')
        if os.path.exists(path) and not os.path.exists(bak):
            shutil.copy2(path, bak)
        with open(path, 'w') as f:
            f.write(content)
        logger.info(f'[admin/agent-prompt] {uid} saved {filename} ({len(content)} chars)')
        return jsonify({'ok': True})
    except Exception as e:
        logger.error(f'[admin/agent-prompt] save error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/strategies', methods=['GET'])
def admin_strategies():
    """All enabled strategies with live config state, today's stats, and funnel summary."""
    uid, err = _admin_auth()
    if err:
        return err
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import SubscriberDB, StrategyControlsDB, EventsDB
        from config import STRATEGY_CONFIG

        try:
            db_rows = {r['strategy_id']: r for r in SubscriberDB.get_all_strategies_pnl()}
        except Exception as _e:
            logger.warning(f"[admin/strategies] get_all_strategies_pnl failed: {_e}")
            db_rows = {}
        try:
            controls = {r['strategy_id']: r for r in StrategyControlsDB.get_all()}
        except Exception as _e:
            logger.warning(f"[admin/strategies] StrategyControlsDB.get_all failed: {_e}")
            controls = {}

        result = []
        for sid, cfg in STRATEGY_CONFIG.items():
            if not cfg.get('enabled', False):
                continue

            db   = db_rows.get(sid, {})
            ctrl = controls.get(sid, {})

            # Resolve live state: DB control takes precedence if row exists
            entry_enabled = ctrl.get('entry_enabled', cfg.get('entry_enabled', False))
            live_enabled  = ctrl.get('live_enabled',  cfg.get('live_enabled',  False))
            mode = 'live' if live_enabled else 'shadow'
            # Same precedence as above: strategy_controls.broker (admin-set,
            # hot-reloaded) wins over config.py's static 'kite' default.
            broker = ctrl.get('broker') or cfg.get('broker', 'kite')

            n      = int(db.get('n_trades') or 0)
            wins   = int(db.get('n_wins') or 0)

            # Today's funnel: last FUNNEL_CYCLE event for this strategy
            funnel = {}
            try:
                rows = EventsDB.get_for_replay(sid,
                    __import__('datetime').date.today(),
                    __import__('datetime').date.today())
                cycle_events = [r for r in rows if r.get('event_type') == 'FUNNEL_CYCLE']
                if cycle_events:
                    last = cycle_events[-1]
                    import json as _json
                    d = last.get('data') or {}
                    if isinstance(d, str):
                        d = _json.loads(d)
                    # Aggregate all today's funnel cycles
                    agg = {'candidates': 0, 'evaluated': 0, 'signals': 0}
                    for ev in cycle_events:
                        evd = ev.get('data') or {}
                        if isinstance(evd, str):
                            evd = _json.loads(evd)
                        for k in agg:
                            agg[k] += evd.get(k, 0)
                    funnel = agg
            except Exception:
                pass

            result.append({
                'strategy_id':   sid,
                'display_name':  cfg.get('description', sid)[:60],
                'mode':          mode,
                'entry_enabled': entry_enabled,
                'live_enabled':  live_enabled,
                'broker':        broker,
                'max_per_trade': cfg.get('max_per_trade', 0),
                'session_start': cfg.get('session_start', ''),
                'session_end':   cfg.get('session_end', ''),
                'n_trades':      n,
                'n_trades_today': int(db.get('n_trades') or 0),
                'win_rate':      round(wins / n, 4) if n else 0,
                'today_pnl_rs':  round(float(db.get('today_pnl_rs') or 0), 2),
                'total_pnl_rs':  round(float(db.get('total_pnl_rs') or 0), 2),
                'open_positions': int(db.get('open_positions') or 0),
                'funnel':        funnel,
                'description':   cfg.get('description', ''),
            })

        return jsonify({'strategies': result})
    except Exception as e:
        logger.error(f"[admin/strategies] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/admin/strategy/<sid>/pause', methods=['POST'])
def admin_strategy_pause(sid):
    uid, err = _admin_auth()
    if err:
        return err
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import StrategyControlsDB
        StrategyControlsDB.set_control(sid.upper(), entry_enabled=False, updated_by=f'owner:{uid}')
        return jsonify({'ok': True, 'strategy_id': sid.upper(), 'entry_enabled': False})
    except Exception as e:
        logger.error(f"[admin/strategy/pause] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/strategy/<sid>/resume', methods=['POST'])
def admin_strategy_resume(sid):
    uid, err = _admin_auth()
    if err:
        return err
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import StrategyControlsDB
        StrategyControlsDB.set_control(sid.upper(), entry_enabled=True, updated_by=f'owner:{uid}')
        return jsonify({'ok': True, 'strategy_id': sid.upper(), 'entry_enabled': True})
    except Exception as e:
        logger.error(f"[admin/strategy/resume] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/strategy/<sid>/arm', methods=['POST'])
def admin_strategy_arm(sid):
    uid, err = _admin_auth()
    if err:
        return err
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import StrategyControlsDB
        StrategyControlsDB.set_control(sid.upper(), live_enabled=True, updated_by=f'owner:{uid}')
        return jsonify({'ok': True, 'strategy_id': sid.upper(), 'live_enabled': True})
    except Exception as e:
        logger.error(f"[admin/strategy/arm] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/strategy/<sid>/disarm', methods=['POST'])
def admin_strategy_disarm(sid):
    uid, err = _admin_auth()
    if err:
        return err
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import StrategyControlsDB
        StrategyControlsDB.set_control(sid.upper(), live_enabled=False, updated_by=f'owner:{uid}')
        return jsonify({'ok': True, 'strategy_id': sid.upper(), 'live_enabled': False})
    except Exception as e:
        logger.error(f"[admin/strategy/disarm] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/strategy/<sid>/broker', methods=['POST'])
def admin_strategy_broker(sid):
    """Set which broker executes this strategy's master-account orders.
    'neo' is a valid selection even though Kotak Neo has no API credentials
    yet — NeoBroker fails closed (raises on any order attempt rather than
    silently no-op'ing or falling back to Kite), so selecting it is safe;
    the strategy's next live order simply fails and logs like any other
    broker rejection. TradeExecutor._resolve_broker() reads this value fresh
    on the strategy's next order — no restart needed."""
    uid, err = _admin_auth()
    if err:
        return err
    body = request.get_json(force=True) or {}
    broker = (body.get('broker') or '').lower()
    if broker not in ('kite', 'neo'):
        return jsonify({'error': f"broker must be 'kite' or 'neo', got {broker!r}"}), 400
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import StrategyControlsDB
        StrategyControlsDB.set_control(sid.upper(), broker=broker, updated_by=f'owner:{uid}')
        return jsonify({'ok': True, 'strategy_id': sid.upper(), 'broker': broker})
    except Exception as e:
        logger.error(f"[admin/strategy/broker] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/executor/arm', methods=['POST'])
def admin_executor_arm():
    uid, err = _admin_auth()
    if err:
        return err
    try:
        with open('/tmp/tradingv2_cmd.txt', 'w') as f:
            f.write('/arm_trading\n')
        logger.info(f"[admin/executor/arm] Command queued by owner:{uid}")
        return jsonify({'ok': True, 'note': 'Command queued — executor will arm within ~5s'})
    except Exception as e:
        logger.error(f"[admin/executor/arm] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/executor/disarm', methods=['POST'])
def admin_executor_disarm():
    uid, err = _admin_auth()
    if err:
        return err
    try:
        with open('/tmp/tradingv2_cmd.txt', 'w') as f:
            f.write('/disarm_trading\n')
        logger.info(f"[admin/executor/disarm] Command queued by owner:{uid}")
        return jsonify({'ok': True, 'note': 'Command queued — executor will disarm within ~5s'})
    except Exception as e:
        logger.error(f"[admin/executor/disarm] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/executor/status', methods=['GET'])
def admin_executor_status():
    uid, err = _admin_auth()
    if err:
        return err
    try:
        import json as _json
        status_file = '/tmp/executor_status.json'
        if os.path.exists(status_file):
            with open(status_file) as f:
                data = _json.load(f)
            return jsonify({'ok': True, **data})
        return jsonify({'ok': True, 'armed': False, 'ts': None,
                        'note': 'No status yet — tradingv2 not started or never armed today'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/subscribers/pending', methods=['GET'])
def admin_subscribers_pending():
    """List broker sessions awaiting owner approval — net-new in phase 1,
    there was previously no way to clear pending_approval other than raw SQL."""
    uid, err = _admin_auth()
    if err:
        return err
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import _exec
        rows = _exec(
            """
            SELECT ubs.user_id, u.username, ubs.broker_type, ubs.created_at
            FROM user_broker_sessions ubs
            JOIN users u ON u.id = ubs.user_id
            WHERE ubs.pending_approval = TRUE
            ORDER BY ubs.created_at ASC
            """,
            fetch='all',
        ) or []
        for r in rows:
            if r.get('created_at'):
                r['created_at'] = r['created_at'].isoformat()
        return jsonify({'pending': rows})
    except Exception as e:
        logger.error(f"[admin/subscribers/pending] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/subscriber/<int:user_id>/approve', methods=['POST'])
def admin_subscriber_approve(user_id):
    """Approve a pending subscriber account — activates their broker session(s)."""
    uid, err = _admin_auth()
    if err:
        return err
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import _exec
        _exec(
            "UPDATE user_broker_sessions SET is_active = TRUE, pending_approval = FALSE "
            "WHERE user_id = %s",
            (user_id,),
        )
        logger.info(f"[admin/subscriber/approve] {uid} approved user_id={user_id}")
        return jsonify({'ok': True})
    except Exception as e:
        logger.error(f"[admin/subscriber/approve] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/subscriber/<int:user_id>/reject', methods=['POST'])
def admin_subscriber_reject(user_id):
    """Reject a pending subscriber account — deactivates their broker session(s)
    so they can't reach the live dashboard. Does not delete their account row."""
    uid, err = _admin_auth()
    if err:
        return err
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import _exec
        _exec(
            "UPDATE user_broker_sessions SET is_active = FALSE, pending_approval = FALSE "
            "WHERE user_id = %s",
            (user_id,),
        )
        logger.info(f"[admin/subscriber/reject] {uid} rejected user_id={user_id}")
        return jsonify({'ok': True})
    except Exception as e:
        logger.error(f"[admin/subscriber/reject] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/positions', methods=['GET'])
def admin_positions():
    """All open master positions with direction + approximate qty. Owner only."""
    uid, err = _admin_auth()
    if err:
        return err
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import SubscriberDB
        positions = SubscriberDB.get_all_open_master_positions()

        # Attempt to get LTPs from Kite
        ltp_map = {}
        try:
            kite = KiteHelper.get_kite_client()
            if kite and positions:
                symbols = list({p['symbol'] for p in positions})
                instruments = [f'NSE:{s}' for s in symbols]
                quotes = kite.quote(instruments) or {}
                ltp_map = {s: quotes.get(f'NSE:{s}', {}).get('last_price', 0) for s in symbols}
        except Exception:
            pass

        result = []
        for p in positions:
            symbol    = p.get('symbol', '')
            zone      = p.get('zone', '')
            entry     = float(p.get('entry_price') or 0)
            capital   = int(p.get('capital_deployed') or 0)
            stop      = float(p.get('stop_price') or 0)
            target    = float(p.get('target_price') or 0)

            direction, qty, ltp, unrl = _position_direction_qty_unrealized(p, ltp_map)

            entry_time = p.get('entry_time')
            result.append({
                'trade_id':    str(p.get('trade_id', '')),
                'strategy_id': p.get('strategy_id', ''),
                'symbol':      symbol,
                'direction':   direction,
                'entry_price': entry,
                'current_price': ltp,
                'stop_price':  stop,
                'target_price': target,
                'qty':         qty,
                'capital':     capital,
                'unrealized_rs': unrl,
                'entry_time':  entry_time.isoformat() if entry_time and hasattr(entry_time, 'isoformat') else str(entry_time or ''),
                'zone':        zone,
            })

        return jsonify({'positions': result})
    except Exception as e:
        logger.error(f"[admin/positions] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/admin/positions/today', methods=['GET'])
def admin_positions_today():
    """Closed positions for today. Owner only."""
    uid, err = _admin_auth()
    if err:
        return err
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import SubscriberDB
        trades = SubscriberDB.get_today_closed()
        for t in trades:
            for key in ('entry_time', 'exit_time'):
                if t.get(key) and hasattr(t[key], 'isoformat'):
                    t[key] = t[key].isoformat()
            zone = t.get('zone', '')
            t['direction'] = 'SHORT' if 'SHORT' in zone.upper() else 'LONG'
        return jsonify({'trades': trades})
    except Exception as e:
        logger.error(f"[admin/positions/today] {e}")
        return jsonify({'error': 'server_error'}), 500


@app.route('/api/admin/position/<trade_id>/exit', methods=['POST'])
def admin_position_exit(trade_id):
    """Force-close an open position: place market order via Kite + mark DB closed. Owner only."""
    uid, err = _admin_auth()
    if err:
        return err
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import TradesV2DB, SubscriberDB

        # Fetch the open position
        positions = SubscriberDB.get_all_open_master_positions()
        pos = next((p for p in positions if str(p.get('trade_id')) == trade_id), None)
        if not pos:
            return jsonify({'error': 'position_not_found'}), 404

        symbol    = pos['symbol']
        zone      = pos.get('zone', '')
        direction = 'SHORT' if 'SHORT' in zone.upper() else 'LONG'
        entry     = float(pos.get('entry_price') or 0)
        capital   = int(pos.get('capital_deployed') or 0)
        qty       = int(capital / entry) if entry > 0 else 0
        if qty <= 0:
            return jsonify({'error': 'cannot_calculate_qty'}), 400

        # Opposite transaction to close
        txn = 'BUY' if direction == 'SHORT' else 'SELL'

        kite = KiteHelper.get_kite_client()
        if not kite:
            return jsonify({'error': 'kite_not_available'}), 503

        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=kite.TRANSACTION_TYPE_BUY if txn == 'BUY' else kite.TRANSACTION_TYPE_SELL,
            quantity=qty,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET,
            validity='DAY',
        )

        # Get approximate fill price from LTP
        try:
            quote = kite.quote([f'NSE:{symbol}'])
            exit_price = quote.get(f'NSE:{symbol}', {}).get('last_price') or entry
        except Exception:
            exit_price = entry

        # Close in DB
        TradesV2DB.manual_close(trade_id, float(exit_price), notes=f'admin_exit by owner:{uid}')

        logger.info(f"[admin/exit] {symbol} qty={qty} {txn} order_id={order_id} exit_price={exit_price}")
        return jsonify({'ok': True, 'order_id': str(order_id), 'symbol': symbol, 'qty': qty, 'exit_price': exit_price})

    except Exception as e:
        logger.error(f"[admin/position/exit] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/position/<trade_id>/2x', methods=['POST'])
def admin_position_double(trade_id):
    """Add a second position of the same size (2x the stake). Owner only."""
    uid, err = _admin_auth()
    if err:
        return err
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import TradesV2DB, SubscriberDB
        import datetime as _dt

        positions = SubscriberDB.get_all_open_master_positions()
        pos = next((p for p in positions if str(p.get('trade_id')) == trade_id), None)
        if not pos:
            return jsonify({'error': 'position_not_found'}), 404

        symbol    = pos['symbol']
        zone      = pos.get('zone', '')
        direction = 'SHORT' if 'SHORT' in zone.upper() else 'LONG'
        entry     = float(pos.get('entry_price') or 0)
        capital   = int(pos.get('capital_deployed') or 0)
        qty       = int(capital / entry) if entry > 0 else 0
        if qty <= 0:
            return jsonify({'error': 'cannot_calculate_qty'}), 400

        txn = 'SELL' if direction == 'SHORT' else 'BUY'

        kite = KiteHelper.get_kite_client()
        if not kite:
            return jsonify({'error': 'kite_not_available'}), 503

        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=symbol,
            transaction_type=kite.TRANSACTION_TYPE_SELL if txn == 'SELL' else kite.TRANSACTION_TYPE_BUY,
            quantity=qty,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET,
            validity='DAY',
        )

        # Get fill price
        try:
            quote = kite.quote([f'NSE:{symbol}'])
            fill_price = quote.get(f'NSE:{symbol}', {}).get('last_price') or entry
        except Exception:
            fill_price = entry

        stop_price   = float(pos.get('stop_price') or 0)
        target_price = float(pos.get('target_price') or 0)
        now_ist = _dt.datetime.now(_dt.timezone.utc)

        # Record in DB as a new trade so the trading loop can manage exits
        new_tid = TradesV2DB.open_trade(
            strategy_id     = pos.get('strategy_id', 'MANUAL'),
            symbol          = symbol,
            entry_time      = now_ist,
            entry_price     = fill_price,
            entry_gap_pct   = 0.0,
            entry_volume_spike = 0.0,
            capital         = capital,
            zone            = zone + '_2X',
            stop_price      = stop_price,
            target_price    = target_price,
            initial_stop_pct   = 0.0,
            initial_target_pct = 0.0,
            metadata        = {'source': 'admin_2x', 'original_trade_id': trade_id, 'kite_order_id': str(order_id)},
        )

        logger.info(f"[admin/2x] {symbol} qty={qty} {txn} order_id={order_id} new_trade={new_tid}")
        return jsonify({'ok': True, 'order_id': str(order_id), 'new_trade_id': new_tid, 'symbol': symbol, 'qty': qty})

    except Exception as e:
        logger.error(f"[admin/position/2x] {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/funnel', methods=['GET'])
def admin_funnel():
    """Today's funnel summary per strategy (aggregated FUNNEL_CYCLE events)."""
    uid, err = _admin_auth()
    if err:
        return err
    try:
        import json as _json
        import datetime as _dt
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import EventsDB
        from config import STRATEGY_CONFIG

        today = _dt.date.today()
        result = {}
        for sid in STRATEGY_CONFIG:
            if not STRATEGY_CONFIG[sid].get('enabled', False):
                continue
            try:
                events = EventsDB.get_for_replay(sid, today, today)
                cycles = [e for e in events if e.get('event_type') == 'FUNNEL_CYCLE']
                if not cycles:
                    result[sid] = None
                    continue
                agg = {'candidates': 0, 'evaluated': 0, 'signals': 0, 'n_cycles': len(cycles)}
                for ev in cycles:
                    d = ev.get('data') or {}
                    if isinstance(d, str):
                        d = _json.loads(d)
                    for k in ('candidates', 'evaluated', 'signals'):
                        agg[k] += d.get(k, 0)
                result[sid] = agg
            except Exception:
                result[sid] = None

        return jsonify({'funnel': result})
    except Exception as e:
        logger.error(f"[admin/funnel] {e}")
        return jsonify({'error': 'server_error'}), 500


# ---------------------------------------------------------------------------
# SPEC PARAM EDITING — read/write key constants in *_spec.py files
# Orchestrator already hot-reloads spec files on mtime change, so writes
# take effect within the next candle cycle (~5s).
# ---------------------------------------------------------------------------

_SPEC_FILE_PATHS = {
    'BLOCK_DEAL_FADE': '/opt/stockapp/tradingv2/strategies/block_deal_fade_spec.py',
    'PVI':             '/opt/stockapp/tradingv2/strategies/pvi_spec.py',
    'BLOCK_DEAL_BOUNCE': '/opt/stockapp/tradingv2/strategies/block_deal_bounce_spec.py',
    'ORB_SHORT':       '/opt/stockapp/tradingv2/strategies/orb_short_spec.py',
    'HA_REVERSAL':     '/opt/stockapp/tradingv2/strategies/ha_reversal_spec.py',
    'ZONE_S21':        '/opt/stockapp/tradingv2/strategies/zone_s21_spec.py',
    'MOMENTUM_DIP':    '/opt/stockapp/tradingv2/strategies/momentum_dip_spec.py',
    'PVI_EOD':         '/opt/stockapp/tradingv2/strategies/pvi_eod_spec.py',
}

# DB-backed params are common across all strategies.
# 'source': 'db' means the value is read from / written to strategy_controls, not the spec file.
_DB_COMMON_PARAMS = [
    {'key': 'capital_scale',  'label': 'Capital scale (× spec capital)',  'type': 'float', 'min': 0.01, 'max': 2,       'group': 'Capital', 'source': 'db'},
    {'key': 'capital_budget', 'label': 'Daily budget (₹)',                'type': 'int',   'min': 10000,'max': 5000000, 'group': 'Capital', 'source': 'db'},
    {'key': 'max_per_trade',  'label': 'Max per trade (₹)',               'type': 'int',   'min': 5000, 'max': 500000,  'group': 'Capital', 'source': 'db'},
    {'key': 'agents_enabled', 'label': 'Agents enabled',                  'type': 'bool',                                'group': 'Agents',  'source': 'db'},
    {'key': 'session_start',  'label': 'Session start (IST HH:MM)',       'type': 'str',                                'group': 'Session', 'source': 'db'},
    {'key': 'session_end',    'label': 'Session end (IST HH:MM)',         'type': 'str',                                'group': 'Session', 'source': 'db'},
]

_SPEC_EDITABLE_PARAMS = {
    'BLOCK_DEAL_FADE': [
        {'key': 'path_a_adv_pct',        'label': 'Path A ADV threshold (%)',     'type': 'float', 'min': 20,   'max': 80,  'group': 'Entry'},
        {'key': 'min_gap_pct',            'label': 'Min gap from open (%)',        'type': 'float', 'min': 0.5,  'max': 5,   'group': 'Entry'},
        {'key': 'min_vwap_dist_pct',      'label': 'Min VWAP distance (%)',        'type': 'float', 'min': 0.5,  'max': 5,   'group': 'Entry'},
        {'key': 'min_vol_pct_short',      'label': 'Min volume % (short)',         'type': 'float', 'min': 0.1,  'max': 2,   'group': 'Entry'},
        {'key': 'short_stop_mult_pct',    'label': 'Stop %',                       'type': 'float', 'min': 0.5,  'max': 3,   'group': 'Risk'},
        {'key': 'strong_target_pct',      'label': 'Strong target %',              'type': 'float', 'min': 2,    'max': 10,  'group': 'Risk'},
        {'key': 'max_concurrent_per_pool','label': 'Max concurrent positions',     'type': 'int',   'min': 1,    'max': 30,  'group': 'Risk'},
        {'key': 'entry_cutoff_ist',       'label': 'Entry cutoff (min of day IST)','type': 'int',   'min': 720,  'max': 930, 'group': 'Risk'},
        {'key': 'bdf_ml_hard_gate',       'label': 'ML entry hard gate',           'type': 'bool',                            'group': 'ML'},
        {'key': 'ml_reject_threshold',    'label': 'ML reject threshold',          'type': 'float', 'min': 0,    'max': 1,   'group': 'ML'},
        {'key': 'bdf_exit_ml_hard_gate',  'label': 'Exit ML hard gate',            'type': 'bool',                            'group': 'ML'},
        {'key': 'exit_ml_exit_threshold', 'label': 'Exit ML threshold',            'type': 'float', 'min': 0,    'max': 1,   'group': 'ML'},
        *_DB_COMMON_PARAMS,
    ],
    'PVI': [
        {'key': 'min_vol_pct',            'label': 'Min volume % of issued shares','type': 'float', 'min': 0.1,  'max': 2,   'group': 'Entry'},
        {'key': 'min_gap_pct',            'label': 'Min gap from open (%)',        'type': 'float', 'min': 0.5,  'max': 5,   'group': 'Entry'},
        {'key': 'gap_gate_enabled',       'label': 'Gap % gate enabled (off = body % only decides the signal candle)',
                                                                                    'type': 'bool',                          'group': 'Entry'},
        {'key': 'min_body_pct',           'label': 'Min green body % of open',     'type': 'float', 'min': 0.5,  'max': 3,   'group': 'Entry'},
        {'key': 'n_reds_gate',            'label': 'Min HA red candles (C1–C4)',   'type': 'int',   'min': 0,    'max': 4,   'group': 'Entry'},
        {'key': 'stop_pct',               'label': 'Stop %',                       'type': 'float', 'min': 0.5,  'max': 3,   'group': 'Risk'},
        {'key': 'max_concurrent',         'label': 'Max concurrent positions',     'type': 'int',   'min': 1,    'max': 20,  'group': 'Risk'},
        {'key': 'entry_cutoff_ist',       'label': 'Entry cutoff (min of day IST)','type': 'int',   'min': 720,  'max': 900, 'group': 'Risk'},
        {'key': 'pvi_exit_ml_threshold',  'label': 'Exit ML threshold',            'type': 'float', 'min': 0,    'max': 1,   'group': 'ML'},
        {'key': 'entry_tranche_swap_vp_gate_enabled', 'label': 'Tranche entry VP-distance gate (unvalidated combo when ON — see spec comment)',
                                                                                    'type': 'bool',                          'group': 'Tranche'},
        {'key': 'shape_manager_leads', 'label': 'Use Shape Manager for live exits (FM becomes shadow)',
                                                                                    'type': 'bool',                          'group': 'Agents', 'source': 'db'},
        *_DB_COMMON_PARAMS,
    ],
    'HA_REVERSAL': [
        {'key': 'ST_PERIOD',              'label': 'Supertrend period',            'type': 'int',   'min': 5,    'max': 20,  'group': 'Signal'},
        {'key': 'ST_MULT',                'label': 'Supertrend multiplier',        'type': 'float', 'min': 2.0,  'max': 5.0, 'group': 'Signal'},
        {'key': 'STOP_PCT',               'label': 'Stop %',                       'type': 'float', 'min': 1,    'max': 5,   'group': 'Risk'},
        {'key': 'MIN_BODY_PCT',           'label': 'Min body % (Heiken Ashi)',     'type': 'float', 'min': 0.1,  'max': 1,   'group': 'Entry'},
        {'key': 'REQUIRE_VOL_CONF',       'label': 'Require volume confirmation',  'type': 'bool',                            'group': 'Entry'},
        *_DB_COMMON_PARAMS,
    ],
    'ZONE_S21': [
        {'key': 'z1_vol',                 'label': 'Zone 1 volume threshold',      'type': 'float', 'min': 0.8,  'max': 2,   'group': 'Entry'},
        {'key': 'z2_vol',                 'label': 'Zone 2 volume threshold',      'type': 'float', 'min': 1.0,  'max': 2.5, 'group': 'Entry'},
        {'key': 'stop_pct',               'label': 'Stop %',                       'type': 'float', 'min': 0.5,  'max': 3,   'group': 'Risk'},
        {'key': 'target_pct',             'label': 'Target %',                     'type': 'float', 'min': 1,    'max': 5,   'group': 'Risk'},
        *_DB_COMMON_PARAMS,
    ],
    'ORB_SHORT': [*_DB_COMMON_PARAMS],
    'FRS':       [*_DB_COMMON_PARAMS],
    'EODR':      [*_DB_COMMON_PARAMS],
    'PVI_EOD': [
        # Capital scale + add-on buy sizing dials (not the rest of PVI_EOD's spec) —
        # these are the DB-backed knobs that actually take live effect without a
        # restart, synced every 30s by orchestrator.py's controls-sync block into
        # instance.spec. capital_scale scales overall D0 position sizing.
        # d0 = mechanical loss-neutralizing buy (~15:25 IST, fires if D0 position down
        # >0.75%). d1 = D1_ADD_AGENT (DeepSeek judgement-based add-on, all of D1).
        # e.g. 0.10 -> the add-on buys 10% of the size it would otherwise compute.
        {'key': 'capital_scale', 'label': 'Capital scale (× spec capital)', 'type': 'float', 'min': 0.01, 'max': 2, 'group': 'Capital', 'source': 'db'},
        {'key': 'pvi_eod_d0_addon_scale', 'label': 'D0 loss-neutralizing buy scale (×)', 'type': 'float', 'min': 0.0, 'max': 1.0, 'group': 'Add-on', 'source': 'db'},
        {'key': 'pvi_eod_d1_addon_scale', 'label': 'D1 add-on agent buy scale (×)',       'type': 'float', 'min': 0.0, 'max': 1.0, 'group': 'Add-on', 'source': 'db'},
        # Entry-window split (2026-07-29, NSE CAS prep — see pvi_eod_spec.py's
        # "Entry window split" comment block). Spec-file-backed (AST read/write,
        # like every other row below without 'source':'db'), hot-reloaded within
        # one cycle of a save, no restart needed. early_entry_pct=0 (default) is
        # a full no-op — the strategy behaves exactly as before this feature.
        {'key': 'early_entry_pct',   'label': 'Early-leg %% of capital (0 = split off, current single-window behavior)', 'type': 'float', 'min': 0.0, 'max': 1.0, 'group': 'Entry window'},
        {'key': 'early_entry_min',   'label': 'Early-leg trigger minute (IST, hour=15 implied, e.g. 0 = 15:00)',          'type': 'int',   'min': 0,   'max': 29,  'group': 'Entry window'},
        {'key': 'auction_entry_min', 'label': 'Remainder-leg trigger minute (IST, hour=15 implied; unchanged default 19 = 15:19)', 'type': 'int', 'min': 0, 'max': 29, 'group': 'Entry window'},
    ],
}


def _read_spec_value(spec_path, key):
    """Extract current value of a constant from a spec file using AST.
    Handles both simple assignments (x = val) and annotated dataclass fields (x: type = val).
    """
    import ast as _ast
    src = open(spec_path).read()
    tree = _ast.parse(src)
    for node in _ast.walk(tree):
        if isinstance(node, _ast.Assign):
            for target in node.targets:
                if isinstance(target, _ast.Name) and target.id == key:
                    try:
                        return _ast.literal_eval(node.value)
                    except Exception:
                        pass
        elif isinstance(node, _ast.AnnAssign):
            if isinstance(node.target, _ast.Name) and node.target.id == key and node.value is not None:
                try:
                    return _ast.literal_eval(node.value)
                except Exception:
                    pass
    return None


def _write_spec_value(spec_path, key, value):
    """Replace a constant assignment in a spec file using regex.
    Handles both simple assignments (key = val) and annotated dataclass fields
    (key: type = val, possibly indented).
    """
    import re as _re, os as _os
    src = open(spec_path).read()
    if isinstance(value, bool):
        new_val = 'True' if value else 'False'
    elif isinstance(value, float):
        new_val = repr(float(value))
    elif isinstance(value, str):
        new_val = repr(value)
    else:
        new_val = repr(int(value))
    # Matches: optional leading whitespace + key + optional ': type' annotation + ' = ' + value
    pattern = rf'^(\s*{_re.escape(key)}\s*(?::[^\n=]*)?\s*=\s*)[^\n#]+'
    replacement = rf'\g<1>{new_val}'
    new_src, count = _re.subn(pattern, replacement, src, flags=_re.MULTILINE)
    if count == 0:
        raise ValueError(f"Key '{key}' not found in {spec_path}")
    open(spec_path, 'w').write(new_src)
    # Touch mtime so orchestrator hot-reload picks up the change
    now = _os.path.getmtime(spec_path)
    _os.utime(spec_path, (now + 1, now + 1))


@app.route('/api/admin/strategy/<sid>/spec-params', methods=['GET'])
def admin_spec_params_get(sid):
    """Return editable spec param definitions + current values for a strategy.
    DB-sourced params ('source': 'db') are read from strategy_controls; spec-sourced
    params are read from the spec file on disk.
    """
    uid, err = _admin_auth()
    if err:
        return err
    sid = sid.upper()
    params_def = _SPEC_EDITABLE_PARAMS.get(sid)
    if params_def is None:
        return jsonify({'error': 'no_spec', 'params': []})
    spec_path = _SPEC_FILE_PATHS.get(sid)
    has_spec_params = any(p.get('source') != 'db' for p in params_def)
    if has_spec_params and spec_path is None:
        return jsonify({'error': 'no_spec', 'params': []})

    from tradingv2.db.queries import StrategyControlsDB as _SCDB
    db_row = {r['strategy_id']: r for r in _SCDB.get_all()}.get(sid, {})

    result = []
    for p in params_def:
        entry = dict(p)
        if p.get('source') == 'db':
            entry['value'] = db_row.get(p['key'])
        else:
            try:
                entry['value'] = _read_spec_value(spec_path, p['key'])
            except Exception:
                entry['value'] = None
        result.append(entry)
    return jsonify({'strategy_id': sid, 'params': result})


@app.route('/api/admin/strategy/<sid>/spec-params', methods=['POST'])
def admin_spec_params_post(sid):
    """Write one or more spec param values. Each key validated against allowed list.
    DB-sourced params ('source': 'db') are written to strategy_controls via set_control().
    Spec-sourced params are written to the spec file on disk.
    """
    uid, err = _admin_auth()
    if err:
        return err
    sid = sid.upper()
    params_def = _SPEC_EDITABLE_PARAMS.get(sid)
    if params_def is None:
        return jsonify({'error': 'no_spec'}), 400
    spec_path = _SPEC_FILE_PATHS.get(sid)
    allowed   = {p['key']: p for p in params_def}
    body      = request.get_json(force=True) or {}
    saved     = []
    errors    = []

    from tradingv2.db.queries import StrategyControlsDB as _SCDB

    for key, raw_val in body.items():
        if key not in allowed:
            errors.append(f"Unknown key: {key}")
            continue
        p = allowed[key]
        try:
            if p['type'] == 'bool':
                val = bool(raw_val)
            elif p['type'] == 'float':
                val = float(raw_val)
                if 'min' in p and val < p['min']:
                    raise ValueError(f"{key}: {val} < min {p['min']}")
                if 'max' in p and val > p['max']:
                    raise ValueError(f"{key}: {val} > max {p['max']}")
            elif p['type'] == 'int':
                val = int(raw_val)
                if 'min' in p and val < p['min']:
                    raise ValueError(f"{key}: {val} < min {p['min']}")
                if 'max' in p and val > p['max']:
                    raise ValueError(f"{key}: {val} > max {p['max']}")
            elif p['type'] == 'str':
                val = str(raw_val).strip()
            else:
                val = raw_val

            if p.get('source') == 'db':
                _SCDB.set_control(sid, **{key: val, 'updated_by': f'owner:{uid}'})
            else:
                if spec_path is None:
                    raise ValueError(f"No spec file for {sid}")
                _write_spec_value(spec_path, key, val)
            saved.append({'key': key, 'value': val})
            logger.info(f"[spec-params] {sid}.{key} = {val!r}  (by owner:{uid})")
        except Exception as e:
            errors.append(f"{key}: {e}")
    return jsonify({'ok': len(errors) == 0, 'saved': saved, 'errors': errors})


@app.route('/api/me', methods=['GET'])
def api_me():
    """Current session user info (role, name, subscriptions) for dashboard init."""
    user_id, name = _sub_session_user()
    if not user_id:
        return jsonify({'error': 'not_logged_in'}), 401
    role = session.get('sub_role', 'subscriber')
    if role in ('owner', 'observer'):
        return jsonify({
            'user_id':       user_id,
            'name':          name,
            'role':          role,
            'username':      session.get('sub_kite_id', name),
            'subscriptions': [],
        })
    try:
        sys.path.insert(0, '/opt/stockapp/tradingv2')
        from db.queries import SubscriberDB
        profile = SubscriberDB.get_subscriber_profile(user_id)
        subs = (profile.get('subscriptions') or []) if profile else []
        return jsonify({
            'user_id':       user_id,
            'name':          name,
            'role':          'subscriber',
            'username':      (profile.get('username', '') if profile else ''),
            'subscriptions': subs,
        })
    except Exception as e:
        logger.error(f"[api/me] {e}")
        return jsonify({'error': 'server_error'}), 500


if __name__ == '__main__':
    try:
        print("🚀 StockMart Simplified API Server - Enhanced")
        print("=" * 60)
        print("Focus: Core broker authentication + portfolio")
        print("Enhanced: Signal handling and service support")
        print("=" * 60)
        
        # NEW: Setup signal handlers for graceful shutdown
        setup_signal_handlers()
        
        # Port configuration
        target_port = 5001
        
        # ENHANCED: Better port check with cleanup
        # BETTER: Smart port check
        logger.info(f"🔍 Smart port check for {target_port}...")
        port_check_result = smart_port_check(target_port)
        if not port_check_result:
            logger.warning(f"⚠️  Port {target_port} check failed, but proceeding anyway (Flask will handle)")
            # Don't exit - let Flask try to bind and fail naturally
            # This prevents restart loops caused by aggressive port cleanup
        else:
            logger.info(f"✅ Port check passed")
        
        # NEW: Network resilience
        # setup_network_resilience()

        # NEW: Initialize connection pool (CONNECTION LEAK FIX)
        logger.info("🔌 Initializing database connection pool...")
        if not initialize_connection_pool():
            logger.error("❌ Connection pool initialization failed")
            sys.exit(1)

        # Initialize database (UNCHANGED)
        logger.info("📊 Initializing database...")
        if not initialize_database():
            logger.error("❌ Database initialization failed")
            sys.exit(1)

        # NEW: Initialize agent system for web interface
        logger.info("🤖 Agent system disabled due to memory constraints")
        agents_initialized = False  # Disable agents to save ~600MB RAM
        # agents_initialized = initialize_agents_for_flask_app()
        if agents_initialized:
            logger.info("✅ Agent system ready for web interface")
        else:
            logger.warning("⚠️ Agent system unavailable - ALADIN chat will be limited")
        
        
        # Validate essential configuration (UNCHANGED)
        required_configs = ['KITE_API_KEY', 'KITE_API_SECRET']
        missing_configs = [config for config in required_configs if not app.config.get(config)]
        
        if missing_configs:
            logger.error(f"❌ Missing required configuration: {', '.join(missing_configs)}")
            logger.error("Please check your .env file")
            sys.exit(1)
        
        # Check Breeze configuration (UNCHANGED)
        breeze_configured = bool(app.config.get('BREEZE_API_KEY') and app.config.get('BREEZE_API_SECRET'))
        
        # Status logging (ENHANCED with new features)
        logger.info("✅ Configuration Status:")
        logger.info(f"   KITE_API_KEY: {'✓' if app.config['KITE_API_KEY'] else '✗'}")
        logger.info(f"   KITE_REDIRECT_URL: {app.config['KITE_REDIRECT_URL']}")
        logger.info(f"   BREEZE_API_KEY: {'✓' if app.config.get('BREEZE_API_KEY') else '✗'}")
        logger.info(f"   BREEZE_REDIRECT_URL: {app.config['BREEZE_REDIRECT_URL']}")
        logger.info(f"   Breeze Integration: {'✅ ENABLED' if breeze_configured else '⚠️ DISABLED'}")
        logger.info(f"   Signal Handlers: ✅ CONFIGURED")  # NEW
        logger.info(f"   Service Mode: ✅ ENABLED")        # NEW
        
        # Endpoint logging (UNCHANGED)
        logger.info("📋 Available Endpoints:")
        logger.info("   Core:")
        logger.info("     POST /api/login")
        logger.info("     GET  /api/health")
        logger.info("     GET  /api/aladin/health")
        # ... rest of your existing endpoint logging
        
        # Find SSL certificates (UNCHANGED)
        ssl_context = find_ssl_certificates()
        
        # Start application with enhanced error handling
        logger.info(f"🚀 Starting server on port {target_port}")
        logger.info("🔧 Service features: Signal handling, graceful shutdown, auto-restart")  # NEW
        logger.info("🧞‍♂️ URL: https://alaidin.info/aladin")

        # Final diagnostic before running
        logger.info(f"🔬 FINAL: About to start Flask app")
        logger.info(f"🔬 FINAL: App instance: {app}")
        logger.info(f"🔬 FINAL: App id: {id(app)}")
        logger.info(f"🔬 FINAL: Has interactive_agent: {hasattr(app, 'interactive_agent')}")
        if hasattr(app, 'interactive_agent'):
            logger.info(f"🔬 FINAL: interactive_agent: {app.interactive_agent}")


        if ssl_context:
            logger.info("🔒 HTTPS mode enabled")
            app.run(
                host='0.0.0.0',
                port=target_port,
                debug=False,
                ssl_context=ssl_context,
                threaded=True,
                use_reloader=False)
        else:
            logger.warning("⚠️ Running without HTTPS")
            app.run(
                host='0.0.0.0',
                port=target_port,
                debug=False,
                threaded=True,
                use_reloader=False
            )



    except KeyboardInterrupt:  # NEW
        logger.info("🛑 Received keyboard interrupt, shutting down...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Application startup failed: {e}")
        sys.exit(1)

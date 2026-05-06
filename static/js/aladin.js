/**
 * ALADIN Streaming Client - COMPLETE FIXED VERSION
 * Version: 2.1 - All Issues Resolved
 * - Image-based charts (no Chart.js)
 * - Proper chat formatting
 * - Analysis data fetching and display
 * - Zoom/pan controls
 */

class ALADINStreamingClient {
    constructor() {
        this.sessionId = null;
        this.isProcessing = false;
        this.eventSource = null;
        this.currentSymbol = null;
        this.currentWorkflowSymbol = null;
        
        // Chart zoom/pan state
        this.chartZoom = 1;
        this.chartPanX = 0;
        this.chartPanY = 0;
        this.isDragging = false;
        this.dragStartX = 0;
        this.dragStartY = 0;
        
        this.config = {
            apiEndpoint: '/api/aladin/chat',
            streamTimeout: 30000,
            retryAttempts: 3,
            retryDelay: 2000
        };
        
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this.initializeApp());
        } else {
            this.initializeApp();
        }
    }

    initializeApp() {
        console.log('🚀 ALADIN v2.1 - Initializing...');
        
        this.generateSessionId();
        this.initializeUIElements();
        this.setupEventListeners();
        this.updateSessionInfo();
        this.setSystemStatus('ready');
        
        this.addChatMessage("Welcome to ALADIN! I can analyze any stock for you. Try asking questions like: 'Will RELIANCE rise next week?', 'How will SIEMENS perform today?', or 'Is it a good time to buy TCS?'", 'assistant');
        
        console.log('✅ Initialization complete');
    }

    generateSessionId() {
        const timestamp = Date.now();
        const randomPart = Math.random().toString(36).substr(2, 8);
        this.sessionId = `web_${timestamp}_${randomPart}`;
    }

    initializeUIElements() {
        this.chatInput = document.getElementById('chat-input');
        this.sendButton = document.getElementById('send-button');
        this.chatHistory = document.getElementById('chat-history');
        this.sessionDisplay = document.getElementById('session-id');
        this.systemStatus = document.getElementById('system-status');
        
        if (!this.chatInput || !this.sendButton || !this.chatHistory) {
            console.error('❌ Required elements not found');
        }
        
        console.log('✅ UI elements initialized');
    }

    setupEventListeners() {
        this.chatInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                this.sendMessage();
            }
        });
        
        this.sendButton.addEventListener('click', () => this.sendMessage());
        
        this.chatInput.addEventListener('input', () => {
            this.chatInput.style.height = 'auto';
            this.chatInput.style.height = Math.min(this.chatInput.scrollHeight, 120) + 'px';
        });
        
        this.chatInput.focus();
    }

    updateSessionInfo() {
        if (this.sessionDisplay) {
            this.sessionDisplay.textContent = this.sessionId.substr(-8);
        }
    }

    setSystemStatus(status) {
        if (!this.systemStatus) return;

        const parentItem = this.systemStatus.closest('.status-item');
        if (parentItem) {
            parentItem.classList.remove('ready', 'processing', 'error');
            parentItem.classList.add(status);
        }

        const statusMap = {
            'ready': '●',
            'processing': '⚡',
            'error': '✖'
        };

        this.systemStatus.textContent = statusMap[status] || '●';
    }

    // =========================================================================
    // AGENT STATUS INDICATORS (Data, Tools, Analysis)
    // =========================================================================

    setAgentStatus(agentName, status, details = '') {
        // Update agent status indicators based on workflow progress
        // agentName: 'data', 'tools', 'analysis'
        // status: 'idle', 'processing', 'complete', 'error', 'ready'
        // details: Optional status details for logging
        const statusMap = {
            'idle': '●',           // Gray dot
            'ready': '●',          // Gray dot
            'processing': '⚡',    // Lightning (processing)
            'complete': '✓',       // Checkmark (done)
            'error': '✖'           // X (error)
        };

        const statusElement = document.getElementById(`${agentName}-status`);
        if (!statusElement) {
            console.warn(`⚠️ Status element for '${agentName}' not found`);
            return;
        }

        const parentItem = statusElement.closest('.agent-item');
        if (parentItem) {
            parentItem.classList.remove('idle', 'ready', 'processing', 'complete', 'error');
            parentItem.classList.add(status);
        }

        statusElement.textContent = statusMap[status] || '●';

        console.log(`📊 [${agentName.toUpperCase()}] ${status.toUpperCase()} ${details ? '- ' + details : ''}`);
    }

    resetAgentStatus() {
        // Reset all agent indicators to idle state
        ['data', 'tools', 'analysis'].forEach(agent => {
            this.setAgentStatus(agent, 'idle', 'Waiting...');
        });
        console.log('🔄 Agent statuses reset to idle');
    }

    updateAgentStatusFromStreamUpdate(update) {
        // Parse stream update and update appropriate agent status
        // Handles messages like:
        // - 'intent_start' → Mark as processing
        // - 'workflow_create' → Mark as processing
        // - 'workflow_started' → Mark data/tools as processing
        // - 'workflow_complete' → Mark analysis as complete
        const type = update.type || '';
        const message = update.message || '';

        // Intent analysis
        if (type === 'intent_start') {
            this.setAgentStatus('data', 'processing', 'Analyzing intent...');
        } else if (type === 'intent_complete') {
            this.setAgentStatus('data', 'complete', 'Intent analyzed');
        }

        // Workflow creation and startup
        if (type === 'workflow_create') {
            this.setAgentStatus('tools', 'processing', 'Preparing tools...');
        } else if (type === 'workflow_started') {
            this.setAgentStatus('data', 'processing', 'Fetching market data...');
            this.setAgentStatus('tools', 'processing', 'Calculating indicators...');
        }

        // Workflow progress - check message content
        if (type === 'workflow_progress' || type === 'workflow_processing') {
            // Parse message to see what's happening
            if (message && message.toLowerCase().includes('data')) {
                this.setAgentStatus('data', 'processing', 'Fetching data...');
            }
            if (message && message.toLowerCase().includes('indicator')) {
                this.setAgentStatus('tools', 'processing', 'Computing indicators...');
            }
            if (message && message.toLowerCase().includes('analysis')) {
                this.setAgentStatus('analysis', 'processing', 'Analyzing...');
            }
        }

        // Workflow heartbeat (still working)
        if (type === 'workflow_heartbeat') {
            this.setAgentStatus('analysis', 'processing', 'Processing...');
        }

        // Completion
        if (type === 'workflow_complete' || type === 'workflow_success') {
            this.setAgentStatus('data', 'complete', 'Data collected');
            this.setAgentStatus('tools', 'complete', 'Tools executed');
            this.setAgentStatus('analysis', 'complete', 'Analysis complete');
        }

        // Error handling
        if (type === 'error' || type === 'workflow_error') {
            // Find which agent failed (if mentioned in message)
            if (message && message.toLowerCase().includes('data')) {
                this.setAgentStatus('data', 'error', message);
            } else if (message && message.toLowerCase().includes('analysis')) {
                this.setAgentStatus('analysis', 'error', message);
            } else {
                // Generic error - mark all as error
                ['data', 'tools', 'analysis'].forEach(agent => {
                    this.setAgentStatus(agent, 'error', message);
                });
            }
        }

        // Timeout
        if (type === 'monitoring_timeout') {
            this.setAgentStatus('analysis', 'error', 'Timeout');
        }
    }

    showStatusUpdate(message, type = 'info') {
        console.log(`📢 ${type}: ${message}`);
        
        const statusDiv = document.createElement('div');
        statusDiv.className = `status-update status-${type}`;
        statusDiv.textContent = message;
        document.body.appendChild(statusDiv);
        
        setTimeout(() => {
            statusDiv.style.opacity = '0';
            setTimeout(() => statusDiv.remove(), 300);
        }, 3000);
    }

    showSystemMessage(message, type = 'info') {
        this.addChatMessage(message, 'system', type);
    }

    setProcessingState(processing) {
        this.isProcessing = processing;
        
        if (this.sendButton) {
            this.sendButton.disabled = processing;
            this.sendButton.textContent = processing ? 'Processing...' : 'Send';
        }
        
        if (this.chatInput) {
            this.chatInput.disabled = processing;
        }
        
        this.setSystemStatus(processing ? 'processing' : 'ready');
    }

    async sendMessage() {
        if (this.isProcessing || !this.chatInput) return;

        const message = this.chatInput.value.trim();
        if (!message) return;

        console.log('💬 Sending:', message);
        this.addChatMessage(message, 'user');

        this.chatInput.value = '';
        this.chatInput.style.height = 'auto';

        // Reset agent status indicators to idle at the start
        this.resetAgentStatus();

        this.setProcessingState(true);
        
        try {
            await this.sendToBackend(message);
        } catch (error) {
            console.error('❌ Error:', error);
            this.showSystemMessage(`Error: ${error.message}`, 'error');
        } finally {
            this.setProcessingState(false);
        }
    }

    async sendToBackend(message) {
        const response = await fetch(this.config.apiEndpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                message: message,
                session_id: this.sessionId
            })
        });

        const contentType = response.headers.get('content-type');
        
        if (contentType && contentType.includes('text/event-stream')) {
            await this.setupEventSource(response);
        } else if (contentType && contentType.includes('application/json')) {
            const data = await response.json();
            
            if (data.type === 'analysis' && data.workflow_initiated) {
                this.showSystemMessage('Analysis workflow started...', 'info');
            } else {
                this.handleRegularResponse(data);
            }
        }
    }

    setupEventSource(response) {
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        this.processStreamChunks(reader, decoder);
    }

    async processStreamChunks(reader, decoder) {
        let buffer = '';
        
        try {
            while (true) {
                const { value, done } = await reader.read();
                
                if (done) {
                    console.log('✅ Stream completed');
                    break;
                }
                
                buffer += decoder.decode(value, { stream: true });
                
                const lines = buffer.split('\n');
                buffer = lines.pop() || '';
                
                for (const line of lines) {
                    this.processStreamLine(line);
                }
            }
        } catch (error) {
            console.error('❌ Stream error:', error);
            this.showSystemMessage(`Stream error: ${error.message}`, 'error');
        }
    }

    processStreamLine(line) {
        if (!line.trim() || !line.startsWith('data: ')) return;
        
        const dataStr = line.substring(6).trim();
        if (dataStr === '[DONE]') return;
        
        try {
            const data = JSON.parse(dataStr);
            this.handleStreamUpdate(data);
        } catch (error) {
            console.warn('⚠️ Parse error:', dataStr);
        }
    }

    handleStreamUpdate(update) {
        console.log('📡 Stream update:', update);

        try {
            // UPDATE AGENT STATUS INDICATORS
            // This ties the stream updates to visual feedback in the header
            this.updateAgentStatusFromStreamUpdate(update);

            if (update.symbol) {
                this.currentWorkflowSymbol = update.symbol;
            }

            // Workflow complete - load chart AND analysis
            if (update.type === 'workflow_complete' || 
                update.type === 'load_chart' ||
                (update.message && update.message.includes('✅') && 
                 update.message.includes('workflow complete'))) {
                
                let symbol = update.symbol || this.currentWorkflowSymbol || 
                           (update.message && this.extractSymbolFromMessage(update.message));
                
                if (symbol) {
                    console.log(`✅ Workflow complete for ${symbol}`);
                    this.showStatusUpdate(`Analysis complete for ${symbol}`, 'success');
                    
                    setTimeout(() => {
                        this.loadAnalysisVisualization(symbol);
                    }, 1000);
                }
            }
            
            // Display messages with proper formatting
            if (update.message) {
                const messageText = typeof update.message === 'string' 
                    ? update.message : JSON.stringify(update.message);
                
                // Agent/workflow messages use system style
                const isAgentMessage = update.agent || update.type === 'workflow_complete' || 
                                     messageText.includes('workflow') || messageText.includes('agent');
                
                if (isAgentMessage) {
                    this.addChatMessage(messageText, 'system', 'info');
                } else {
                    this.addChatMessage(messageText, 'assistant');
                }
            }
            
        } catch (error) {
            console.error('❌ Error handling stream:', error);
        }
    }

    extractSymbolFromMessage(message) {
        if (!message) return null;

        const match = message.match(/workflow complete for ([A-Z]+)/i) ||
                      message.match(/\b([A-Z]{2,10})\b/);
        return match ? match[1] : null;
    }

    handleInterruption(update) {
        console.log('🎯 Handling interruption:', update);

        const frontendAction = update.frontend_action;
        const message = update.message;

        // Always show the response message
        if (message) {
            this.addChatMessage(message, 'assistant');
        }

        // Handle different frontend actions
        switch (frontendAction) {
            case 'disconnect_all':
                // Cancel current workflow - clear everything
                this.disconnectWorkflow('cancelled');
                this.showStatusUpdate('Analysis cancelled', 'warning');
                break;

            case 'disconnect_and_prepare_new':
                // Cancel current and prepare for new workflow
                this.disconnectWorkflow('preparing_new');
                this.showStatusUpdate('Switching to new analysis...', 'info');

                // If there's a new workflow to trigger, send it
                if (update.trigger_new_workflow && update.new_message) {
                    setTimeout(() => {
                        this.sendMessage(update.new_message);
                    }, 500);
                }
                break;

            case 'show_chat_only':
                // General chat response - don't disconnect workflow
                // Just show the message (already done above)
                console.log('💭 Continuing workflow with chat response');
                break;

            case 'show_patience_message':
                // Out of scope - ask for patience
                this.showStatusUpdate('Please wait for current analysis to complete', 'info');
                break;

            default:
                console.log('No specific frontend action required');
        }
    }

    disconnectWorkflow(reason = 'cancelled') {
        /**
         * Disconnect frontend from current workflow stream
         * Clears chart, summary, and prepares for new workflow
         */
        console.log(`🔌 Disconnecting workflow: ${reason}`);

        // Stop listening to current workflow symbol
        this.currentWorkflowSymbol = null;

        // Clear chart container
        const chartContainer = document.getElementById('chart-container');
        if (chartContainer) {
            chartContainer.innerHTML = `
                <div style="text-align: center; padding: 20px; color: #90A4AE;">
                    <p>${reason === 'preparing_new' ?
                        '🔄 Preparing new analysis...' :
                        '📊 Chart cleared - Ready for new analysis'}</p>
                </div>
            `;
        }

        // Clear summary section
        const summaryElement = document.getElementById('analysis-summary-container');
        if (summaryElement) {
            summaryElement.innerHTML = reason === 'preparing_new' ?
                '<p style="text-align: center; color: #90A4AE;">⏳ New analysis starting...</p>' :
                '<p style="text-align: center; color: #90A4AE;">No active analysis</p>';
            summaryElement.style.display = 'block';
        }

        // Clear/reset analysis data
        this.analysisData = null;

        // Reset agent status if workflow cancelled (not preparing new)
        if (reason === 'cancelled') {
            this.resetAgentStatus();
        }

        console.log('✅ Workflow disconnected, frontend cleared');
    }

    resetAgentStatus() {
        /**
         * Reset all agent status indicators to idle state
         */
        ['data', 'tools', 'analysis'].forEach(agent => {
            this.setAgentStatus(agent, 'idle', 'Waiting...');
        });
    }

    handleRegularResponse(data) {
        if (data.error) {
            this.showSystemMessage(data.error, 'error');
            return;
        }
        
        const responseText = this.extractResponseText(data);
        if (responseText) {
            this.addChatMessage(responseText, 'assistant');
        }
        
        if (data.session_id && data.session_id !== this.sessionId) {
            this.sessionId = data.session_id;
            this.updateSessionInfo();
        }
    }

    extractResponseText(data) {
        const fields = ['chat_response', 'response', 'message', 'result', 'answer', 'content', 'text', 'output'];
        
        for (const field of fields) {
            if (data[field] && typeof data[field] === 'string' && data[field].trim()) {
                return data[field];
            }
        }
        
        if (data.result && typeof data.result === 'object') {
            for (const field of fields) {
                if (data.result[field] && typeof data.result[field] === 'string') {
                    return data.result[field];
                }
            }
        }
        
        return null;
    }

    addChatMessage(text, role = 'assistant', type = 'normal') {
        if (!this.chatHistory) return;
        
        const messageDiv = document.createElement('div');
        messageDiv.className = `chat-message ${role}`;
        
        if (type !== 'normal') {
            messageDiv.classList.add(type);
        }
        
        if (role === 'user') {
            messageDiv.innerHTML = `<strong>You:</strong> ${this.escapeHtml(text)}`;
        } else {
            // Both assistant AND system now use formatAssistantMessage
            messageDiv.innerHTML = this.formatAssistantMessage(text);
        }
        
        messageDiv.setAttribute('data-timestamp', new Date().toLocaleTimeString());
        
        this.chatHistory.appendChild(messageDiv);
        this.scrollToBottom();
        
        return messageDiv;
    }



    formatAssistantMessage(content) {
        return `<span style="font-size: 1.25em;"><span style="font-weight: 300;">al</span><strong>ai</strong><span style="font-weight: 300;">din</span></span>: ${this.formatMessage(content)}`;
    }

    formatMessage(text) {
        if (!text) return '';
        return text
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/\n/g, '<br>');
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    scrollToBottom() {
        if (this.chatHistory) {
            this.chatHistory.scrollTop = this.chatHistory.scrollHeight;
        }
    }

    // =========================================================================
    // CHART AND ANALYSIS LOADING
    // =========================================================================

    async fetchChartImage(symbol) {
        try {
            const response = await fetch(`/api/aladin/chart/latest/${symbol}`);
            const result = await response.json();
            
            if (result.success && result.chart_url) {
                return {
                    success: true,
                    chartUrl: result.chart_url,
                    filename: result.filename,
                    metadata: result.metadata
                };
            }
            
            return { success: false, error: result.error || 'Chart not available' };
        } catch (error) {
            return { success: false, error: error.message };
        }
    }

    displayChartImage(chartData) {
        try {
            const chartContainer = document.getElementById('chart-container');
            const chartPlaceholder = document.getElementById('chart-placeholder');
            
            if (!chartContainer) return false;
            
            if (chartPlaceholder) chartPlaceholder.style.display = 'none';
            chartContainer.style.display = 'block';
            chartContainer.innerHTML = '';
            
            // BOLD symbol bar - no background, no borders, large text
            const controlBar = document.createElement('div');
            controlBar.style.cssText = 'display: flex; justify-content: space-between; align-items: center; margin: 0 0 4px; padding: 0;';


            // Left side - BOLD metadata
            const metadataDiv = document.createElement('div');
            metadataDiv.id = 'chart-metadata';
            metadataDiv.style.cssText = 'display: flex; gap: 20px; color: #37474F; font-weight: 700; font-size: 16px;';
            metadataDiv.innerHTML = `
                <span style="font-size: 18px; color: #dc3545;"><span id="symbol-display">${this.currentSymbol || 'N/A'}</span></span>
                <span id="analysis-period"> <span style="color: #4A6C8C;">-</span></span>
                <span id="target-display">Target: <span style="color: #4A6C8C;">-</span></span>
                <span id="entry-display">Entry: <span style="color: #4A6C8C;">-</span></span>
                <span id="confidence-display">Confidence: <span style="color: #4A6C8C;">-</span></span>
            `;
            
            // Right side - compact controls NO borders
            const controlsDiv = document.createElement('div');
            controlsDiv.style.cssText = 'display: flex; gap: 3px; align-items: center;';
            
            const zoomLabel = document.createElement('span');
            zoomLabel.id = 'zoom-label';
            zoomLabel.style.cssText = 'font-size: 14px; color: #37474F; font-weight: 600; margin-right: 4px;';
            zoomLabel.textContent = '100%';
            
            const zoomInBtn = this.createCompactButton('+', () => this.zoomChart(1.2));
            const zoomOutBtn = this.createCompactButton('−', () => this.zoomChart(0.8));
            const popOutBtn = this.createCompactButton('↗', () => window.open(chartData.chartUrl, '_blank'));
            
            controlsDiv.append(zoomLabel, zoomInBtn, zoomOutBtn, popOutBtn);
            controlBar.append(metadataDiv, controlsDiv);
            chartContainer.appendChild(controlBar);
            
            // After chartContainer.appendChild(controlBar);
            if (this.analysisData?.action) {
                const colors = { 'buy': '#28a745', 'sell': '#dc3545', 'hold': '#ffc107' };
                const color = colors[this.analysisData.action.toLowerCase()] || '#6c757d';
                const actionBadge = document.createElement('div');
                actionBadge.style.cssText = 'text-align: center; margin-bottom: 8px;';
                actionBadge.innerHTML = `<span style="padding: 6px 16px; background: ${color}; color: white; border-radius: 4px; font-weight: bold; font-size: 0.9em; text-transform: uppercase;">${this.analysisData.action}</span>`;
                chartContainer.appendChild(actionBadge);
            }
            // Image wrapper - SINGLE scroll
            const imageWrapper = document.createElement('div');
            imageWrapper.id = 'chart-image-wrapper';
            imageWrapper.style.cssText = `
                width: 100%;
                overflow: visible;  /* Changed from auto */
                position: relative;
                border: 0px solid #ddd;
                border-radius: 2px;
                cursor: grab;
                background: #f9f9f9;
            `;
            
            const img = document.createElement('img');
            img.id = 'chart-image';
            img.src = chartData.chartUrl;
            img.alt = `Chart for ${this.currentSymbol}`;
            img.style.cssText = `
                display: block;
                width: 100%;  /* Fit to container width */
                height: auto;
                transform-origin: top left;
                transition: transform 0.2s ease;
            `;
            
            // Pan/drag (unchanged)
            imageWrapper.addEventListener('mousedown', (e) => {
                this.isDragging = true;
                this.dragStartX = e.clientX - this.chartPanX;
                this.dragStartY = e.clientY - this.chartPanY;
                imageWrapper.style.cursor = 'grabbing';
            });
            
            imageWrapper.addEventListener('mousemove', (e) => {
                if (this.isDragging) {
                    this.chartPanX = e.clientX - this.dragStartX;
                    this.chartPanY = e.clientY - this.chartPanY;
                    this.updateChartTransform();
                }
            });
            
            imageWrapper.addEventListener('mouseup', () => {
                this.isDragging = false;
                imageWrapper.style.cursor = 'grab';
            });
            
            imageWrapper.addEventListener('mouseleave', () => {
                this.isDragging = false;
                imageWrapper.style.cursor = 'grab';
            });
            
            imageWrapper.addEventListener('wheel', (e) => {
                e.preventDefault();
                this.zoomChart(e.deltaY > 0 ? 0.9 : 1.1);
            });
            
            img.onload = () => console.log('✅ Chart loaded');
            img.onerror = () => this.showChartPlaceholder('Chart failed to load');
            
            imageWrapper.appendChild(img);
            chartContainer.appendChild(imageWrapper);
            
            // Update button styles - NO BORDERS, larger text
            if (!document.getElementById('compact-btn-styles')) {
                const style = document.createElement('style');
                style.id = 'compact-btn-styles';
                style.textContent = `
                    .chart-compact-btn {
                        padding: 6px 14px;
                        border: none;
                        border-radius: 4px;
                        background: transparent;
                        color: #37474F;
                        cursor: pointer;
                        font-size: 18px;
                        font-weight: 700;
                        transition: all 0.15s;
                        min-width: 38px;
                    }
                    .chart-compact-btn:hover { background: #e0e0e0; }
                    .chart-compact-btn:active { transform: scale(0.95); }
                `;
                document.head.appendChild(style);
            }
            
            return true;
        } catch (error) {
            console.error('❌ Display error:', error);
            return false;
        }
    }

    createCompactButton(text, onClick) {
        const btn = document.createElement('button');
        btn.innerHTML = text;
        btn.className = 'chart-compact-btn';
        btn.onclick = onClick;
        return btn;
    }

    zoomChart(factor) {
        this.chartZoom *= factor;
        this.chartZoom = Math.max(0.5, Math.min(5, this.chartZoom));
        this.updateChartTransform();
        
        const label = document.getElementById('zoom-label');
        if (label) {
            label.textContent = `${(this.chartZoom * 100).toFixed(0)}%`;
        }
    }

    createButton(text, onClick) {
        const btn = document.createElement('button');
        btn.innerHTML = text;
        btn.className = 'chart-btn';
        btn.onclick = onClick;
        return btn;
    }

    zoomChart(factor) {
        this.chartZoom *= factor;
        this.chartZoom = Math.max(0.5, Math.min(5, this.chartZoom));
        this.updateChartTransform();
        
        const label = document.getElementById('zoom-label');
        if (label) {
            label.textContent = `Zoom: ${(this.chartZoom * 100).toFixed(0)}%`;
        }
    }

    resetChartView() {
        this.chartZoom = 1;
        this.chartPanX = 0;
        this.chartPanY = 0;
        this.updateChartTransform();
        
        const label = document.getElementById('zoom-label');
        if (label) label.textContent = 'Zoom: 100%';
    }

    updateChartTransform() {
        const img = document.getElementById('chart-image');
        if (img) {
            img.style.transform = `scale(${this.chartZoom}) translate(${this.chartPanX / this.chartZoom}px, ${this.chartPanY / this.chartZoom}px)`;
        }
    }

    showChartPlaceholder(message = 'No chart available') {
        const chartContainer = document.getElementById('chart-container');
        const chartPlaceholder = document.getElementById('chart-placeholder');
        
        if (chartContainer) chartContainer.style.display = 'none';
        
        if (chartPlaceholder) {
            chartPlaceholder.style.display = 'flex';
            chartPlaceholder.innerHTML = `
                <div style="text-align: center; padding: 40px;">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 150px; height: 150px; opacity: 0.3; margin-bottom: 20px;">
                        <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"></polyline>
                    </svg>
                    <p style="color: #666; font-size: 1.1em;">${message}</p>
                    <p style="color: #999; font-size: 0.9em;">Try running an analysis to generate a chart</p>
                </div>
            `;
        }
    }

    async fetchAndDisplayAnalysis(symbol) {
        try {
            const response = await fetch(`/api/aladin/analysis/${symbol}/latest`);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const result = await response.json();
            const data = result.data || result;
            
            if (!data || Object.keys(data).length === 0) {
                throw new Error('No analysis data available');
            }
            
            const container = document.getElementById('analysis-summary-container');
            const placeholder = document.getElementById('analysis-placeholder');
            
            if (!container) return;
            
            if (placeholder) placeholder.style.display = 'none';
            container.style.display = 'block';
            
            // Update chart metadata
            this.updateChartMetadata(data);
            
            // Build HTML with consistent Aladin formatting
            let html = '';
            
            // Current Price Header (with fallback)
            const currentPrice = data.current_price || data.close || data.last_price;
            if (currentPrice) {
                html += `<div style="padding: 4px 10px; background: #ECEFF1; margin-bottom: 4px; border-radius: 4px;">`;
                html += `<span style="font-size: 0.9em; color: #37474F;"><strong>Current Price:</strong> ₹${parseFloat(currentPrice).toFixed(2)}</span>`;
                
                if (data.analysis_timestamp) {
                    const timestamp = new Date(data.analysis_timestamp).toLocaleDateString('en-IN', {
                        year: 'numeric', month: 'short', day: 'numeric'
                    });
                    html += ` <span style="color: #90A4AE; margin-left: 8px;">• ${timestamp}</span>`;
                }
                html += `</div>`;
            }
            
            // Summary Section (with consistent Aladin styling)
            // Line 669: Analysis summary display (standardized)
            const summaryText = data.executive_summary || data.summary || '';


            if (summaryText) {
                html += `<div style="padding: 6px 10px; background: white; margin-bottom: 4px; border-left: 3px solid #2C8C6A; border-radius: 2px;">`;
                html += `<h4 style="margin: 0 0 4px 0; font-size: 0.95rem; color: #2C8C6A; font-weight: 600;">📋 Summary</h4>`;
                html += `<div style="font-size: 0.9em; line-height: 1.5; color: #37474F;">${this.formatMessage(summaryText)}</div>`;
                html += `</div>`;
            }
            
            // Recommendation Section (matching Summary style)
            // Line 679: Recommendation text extraction (standardized)
            const recommendationText = data.trading_recommendations || data.recommendation || '';
            if (recommendationText) {
                html += `<div style="padding: 6px 10px; background: white; margin-bottom: 4px; border-left: 3px solid #4A6C8C; border-radius: 2px;">`;
                html += `<h4 style="margin: 0 0 4px 0; font-size: 0.95rem; color: #4A6C8C; font-weight: 600;">💡 Recommendation</h4>`;
                html += `<div style="font-size: 0.9em; line-height: 1.5; color: #37474F;">${this.formatMessage(recommendationText)}</div>`;
                html += `</div>`;
            }
            
            // Display or show empty state
            container.innerHTML = html || '<div style="padding: 12px; text-align: center; color: #90A4AE;">No analysis details available</div>';
            
            // Add Research Report Button
            this.createResearchReportButton(data);
            
        } catch (error) {
            console.error('Failed to load analysis:', error.message);
            
            const container = document.getElementById('analysis-summary-container');
            if (container) {
                container.innerHTML = `
                    <div style="padding: 12px; text-align: center; color: #E54B4B;">
                        <p style="margin: 0;">⚠️ Unable to load analysis</p>
                        <p style="margin: 4px 0 0 0; font-size: 0.85em; color: #90A4AE;">${error.message}</p>
                    </div>
                `;
            }
            
            this.showStatusUpdate(`Failed to load analysis: ${error.message}`, 'error');
        }
    }

    updateChartMetadata(data) {
        console.log('🔍 Updating metadata:', data);

        // Period from outlook_period (forward-looking prediction horizon, not lookback period)
        const periodEl = document.getElementById('analysis-period');
        if (periodEl) {
            if (data.outlook_period) {
                periodEl.innerHTML = `<span style="color: #666;">${data.outlook_period}d</span>`;
                console.log(`✅ Using outlook_period: ${data.outlook_period}d`);
            } else if (data.timeframe) {
                // Fallback to timeframe if outlook_period not available
                periodEl.innerHTML = `<span style="color: #666;">~${data.timeframe}d</span>`;
                console.log(`⚠️ Fallback to timeframe: ${data.timeframe}d (outlook_period not available)`);
            }
        }
        
        // Target
        if (data.targets?.[0]) {
            const targetEl = document.getElementById('target-display');
            if (targetEl) targetEl.innerHTML = `Target: <span style="color: #666;">₹${data.targets[0]}</span>`;
        }
        
        // Entry
        if (data.entry_points?.[0]) {
            const entryEl = document.getElementById('entry-display');
            const entry = data.entry_points;
            if (entryEl) entryEl.innerHTML = `Entry: <span style="color: #666;">₹${entry[0]}${entry[1] ? `-${entry[1]}` : ''}</span>`;
        }
        
        // Confidence from dominant pattern
        if (data.chart_visualization_data?.dominant_pattern?.confidence) {
            const confEl = document.getElementById('confidence-display');
            if (confEl) confEl.innerHTML = `Confidence: <span style="color: #666;">${data.chart_visualization_data.dominant_pattern.confidence}%</span>`;
        }
    }

    async loadAnalysisVisualization(symbol) {
        try {
            this.currentSymbol = symbol;
            console.log(`📊 Loading visualization for ${symbol}`);
            
            this.showStatusUpdate(`Loading data for ${symbol}...`, 'info');
            this.setSystemStatus('processing');
            
            // Fetch BOTH in parallel for speed
            const [chartResult, analysisResponse] = await Promise.all([
                this.fetchChartImage(symbol),
                fetch(`/api/aladin/analysis/${symbol}/latest`)
            ]);
            
            const analysisData = await analysisResponse.json();
            
            // Display chart first (creates DOM)
            if (chartResult.success) {
                this.displayChartImage(chartResult);
                // NOW update metadata after DOM exists
                this.updateChartMetadata(analysisData.data);
            } else {
                console.warn(`⚠️ No chart for ${symbol}`);
                this.showChartPlaceholder(`No chart available for ${symbol}`);
            }
            
            // Display analysis
            await this.fetchAndDisplayAnalysis(symbol);
            
            this.setSystemStatus('ready');
            
        } catch (error) {
            console.error(`❌ Visualization error:`, error);
            this.showStatusUpdate(`Error: ${error.message}`, 'error');
            this.showChartPlaceholder('Failed to load');
            this.setSystemStatus('error');
        }
    }

    // ========================================================================
    // NEW FUNCTIONS FOR RESEARCH REPORT MODAL
    // ADD THESE TO ALADINStreamingClient class in aladin.js
    // Insert before the closing brace of the class (before "// Initialize" section)
    // ========================================================================
    createResearchReportButton(data) {
        // Remove existing button if present
        const existingBtn = document.getElementById('research-report-btn');
        if (existingBtn) existingBtn.remove();

        // Remove old modal so new analysis data triggers fresh report generation
        const existingModal = document.getElementById('research-report-modal');
        if (existingModal) existingModal.remove();

        // Create button container - positioned at bottom-right
        const analysisContainer = document.getElementById('analysis-summary-container');
        if (!analysisContainer) return;

        const btnContainer = document.createElement('div');
        btnContainer.id = 'research-report-btn';
        btnContainer.style.cssText = `
            position: absolute;
            bottom: 80px;
            right: 12px;
            z-index: 5;
        `;

        const button = document.createElement('button');
        button.innerHTML = '📄 Comprehensive Report';
        button.style.cssText = `
            padding: 10px 18px;
            background: linear-gradient(135deg, #4A6C8C 0%, #2C5282 100%);
            color: #FFFFFF;
            border: none;
            border-radius: 6px;
            font-size: 0.95rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            box-shadow: 0 4px 12px rgba(74, 108, 140, 0.3);
        `;

        button.onmouseover = () => {
            button.style.background = 'linear-gradient(135deg, #3A5A7C 0%, #1C4272 100%)';
            button.style.boxShadow = '0 6px 16px rgba(74, 108, 140, 0.4)';
            button.style.transform = 'translateY(-2px)';
        };
        button.onmouseout = () => {
            button.style.background = 'linear-gradient(135deg, #4A6C8C 0%, #2C5282 100%)';
            button.style.boxShadow = '0 4px 12px rgba(74, 108, 140, 0.3)';
            button.style.transform = 'translateY(0)';
        };
        
        button.onclick = () => this.toggleResearchReport(data);
        
        btnContainer.appendChild(button);
        
        // Make analysis container position relative so button can be absolutely positioned
        analysisContainer.style.position = 'relative';
        analysisContainer.appendChild(btnContainer);
    }
// ========================================================================
// COMPLETE RESEARCH REPORT - Professional Financial Report Format
// ========================================================================
// REPLACES: createResearchReportModal() and toggleResearchReport() functions
// This creates a comprehensive financial research report using ALL database fields
// ========================================================================

    toggleResearchReport(data) {
        let modal = document.getElementById('research-report-modal');

        if (modal) {
            // Check if data is newer (has different analysis timestamp)
            const existingTimestamp = modal.dataset.analysisTimestamp;
            const newTimestamp = data.analysis_timestamp;

            if (existingTimestamp && newTimestamp && existingTimestamp !== newTimestamp) {
                // New analysis data - replace the old modal
                modal.remove();
                modal = null;
            } else if (existingTimestamp === newTimestamp) {
                // Same data - just toggle visibility
                if (modal.style.display === 'none') {
                    modal.style.display = 'flex';
                } else {
                    modal.style.display = 'none';
                }
                return;
            }
        }

        if (!modal) {
            // Create new modal
            modal = this.createComprehensiveResearchReport(data);
            document.body.appendChild(modal);
        } else {
            // Toggle if modal still exists
            if (modal.style.display === 'none') {
                modal.style.display = 'flex';
            } else {
                modal.style.display = 'none';
            }
        }
    }

    createComprehensiveResearchReport(data) {
        // ===== DEBUG: Log incoming data =====
        console.log('🔍 [RESEARCH REPORT] Incoming data:', data);
        console.log('🔍 [RESEARCH REPORT] Keys:', Object.keys(data));
        
        // ===== HELPER: Extract JSONB fields =====
        const extractJsonbField = (value) => {
            if (!value) return {};
            if (typeof value === 'string') {
                try {
                    return JSON.parse(value);
                } catch (e) {
                    return {};
                }
            }
            return value;
        };

        // ===== EXTRACT JSONB FIELDS =====
        const chart_viz_data = extractJsonbField(data.chart_visualization_data);
        const support_levels = extractJsonbField(data.support_levels);
        const resistance_levels = extractJsonbField(data.resistance_levels);
        const entry_points = extractJsonbField(data.entry_points);
        const targets = extractJsonbField(data.targets);
        let stop_loss = extractJsonbField(data.stop_loss);

        // If stop_loss is empty, try to extract from targets array or chart_viz_data
        if (!stop_loss || Object.keys(stop_loss).length === 0) {
            // Try to get from chart_viz_data
            if (chart_viz_data && chart_viz_data.stop_loss) {
                stop_loss = extractJsonbField(chart_viz_data.stop_loss);
            }
            // If still empty, try to get from targets if it has stop_loss info
            else if (Array.isArray(targets) && targets.length > 0) {
                const targetWithStopLoss = targets.find(t => t.stop_loss);
                if (targetWithStopLoss && targetWithStopLoss.stop_loss) {
                    stop_loss = targetWithStopLoss.stop_loss;
                }
            }
        }

        // ===== BUILD REPORT DATA (using actual database field names) =====
        const reportData = {
            // Identifier
            symbol: data.symbol || this.currentSymbol || 'N/A',
            
            // ✅ TEXT FIELDS - Database column names with fallbacks
            summary: data.executive_summary || data.summary || 'No summary available',                          // API returns: executive_summary (fallback to summary)
            details: data.detailed_analysis || data.details || '',                          // API returns: detailed_analysis (fallback to details)
            trading_recommendations: data.trading_recommendations || data.recommendation || '',
            market_context: data.market_context || '',            // Database: market_context (if exists)
            
            // ✅ TRADING FIELDS - Database column names
            current_price: data.targets || 'N/A',           // Map to targets (current_price not available in DB)
            action: data.action || 'N/A',                         // Database: action
            conviction: data.conviction || 'N/A',                 // Database: conviction
            target_timeframe: data.target_timeframe || 'N/A',     // Database: target_timeframe
            trend_direction: data.trend_direction || '',          // Database: trend_direction
            
            // ✅ PRICE LEVELS - From JSONB fields or chart_visualization_data
            support_levels: Array.isArray(support_levels) ? support_levels :
                        (chart_viz_data.support_levels || []),
            resistance_levels: Array.isArray(resistance_levels) ? resistance_levels :
                            (chart_viz_data.resistance_levels || []),
            entry_points: Array.isArray(entry_points) ? entry_points :
                        (chart_viz_data.entry_points || []),
            targets: Array.isArray(targets) ? targets :
                    (chart_viz_data.targets || []),
            stop_loss: stop_loss || chart_viz_data.stop_loss,
            
            // ✅ PATTERNS & INDICATORS
            patterns: chart_viz_data.patterns || chart_viz_data.high_level_patterns || [],
            indicator_signals: chart_viz_data.indicator_signals || {},
            
            // ✅ METADATA
            analysis_timestamp: data.analysis_timestamp || '',
            indicators_used: data.indicators_used || [],
            data_points: data.data_points || 0
        };

        console.log('📊 [RESEARCH REPORT] Processed reportData:', reportData);
        
        // ===== CREATE MODAL =====
        const modal = document.createElement('div');
        modal.id = 'research-report-modal';
        modal.style.cssText = `
            display: flex;
            position: fixed;
            top: 0;
            left: 0;
            width: 100vw;
            height: 100vh;
            background: rgba(0, 0, 0, 0.7);
            z-index: 1000;
            align-items: center;
            justify-content: center;
            padding: 20px;
        `;
        
        modal.onclick = (e) => {
            if (e.target === modal) {
                modal.style.display = 'none';
            }
        };
        
        // ===== REPORT CONTAINER =====
        const reportContainer = document.createElement('div');
        reportContainer.style.cssText = `
            background: white;
            width: 100%;
            max-width: 1100px;
            max-height: 90vh;
            border-radius: 12px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.4);
            display: flex;
            flex-direction: column;
            overflow: hidden;
        `;
        reportContainer.onclick = (e) => e.stopPropagation();
        
        // ===== HEADER =====
        const header = document.createElement('div');
        header.style.cssText = `
            background: linear-gradient(135deg, #2C8C6A, #4A6C8C);
            color: white;
            padding: 20px 30px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 3px solid #1a5c4a;
        `;
        
        const timestamp = reportData.analysis_timestamp ? 
            new Date(reportData.analysis_timestamp).toLocaleDateString('en-US', {
                year: 'numeric', month: 'long', day: 'numeric'
            }) : new Date().toLocaleDateString('en-US', {
                year: 'numeric', month: 'long', day: 'numeric'
            });
        
        header.innerHTML = `
            <div>
                <h1 style="margin: 0; font-size: 1.8rem; font-weight: 700;">Research Report: ${reportData.symbol}</h1>
                <p style="margin: 5px 0 0 0; font-size: 0.95rem; opacity: 0.9;">${timestamp}</p>
            </div>
            <button id="modal-close-btn" style="
                background: rgba(255,255,255,0.2);
                border: 2px solid white;
                color: white;
                font-size: 1.8rem;
                width: 40px;
                height: 40px;
                border-radius: 50%;
                cursor: pointer;
                line-height: 1;
                transition: all 0.2s;
            ">×</button>
        `;
        
        // ===== CONTENT AREA =====
        const contentArea = document.createElement('div');
        contentArea.style.cssText = `
            flex: 1;
            overflow-y: auto;
            padding: 30px 40px;
            background: #fafafa;
        `;
        
        // ===== BUILD REPORT HTML =====
        let reportHTML = '';
        
        // 1. KEY METRICS HEADER
        reportHTML += `
            <div style="background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #2C8C6A;">
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px;">
                    <div>
                        <span style="color: #90A4AE; font-size: 0.85rem; text-transform: uppercase;">Current Price</span>
                        <div style="font-size: 1.5rem; font-weight: 700; color: #37474F;">₹${reportData.current_price}</div>
                    </div>
                    <div>
                        <span style="color: #90A4AE; font-size: 0.85rem; text-transform: uppercase;">Action</span>
                        <div style="font-size: 1.3rem; font-weight: 700; color: ${reportData.action === 'buy' ? '#2C8C6A' : reportData.action === 'sell' ? '#E54B4B' : '#F59E0B'}; text-transform: uppercase;">${reportData.action}</div>
                    </div>
                    <div>
                        <span style="color: #90A4AE; font-size: 0.85rem; text-transform: uppercase;">Conviction</span>
                        <div style="font-size: 1.3rem; font-weight: 700; color: #4A6C8C; text-transform: capitalize;">${reportData.conviction}</div>
                    </div>
                    <div>
                        <span style="color: #90A4AE; font-size: 0.85rem; text-transform: uppercase;">Timeframe</span>
                        <div style="font-size: 1.3rem; font-weight: 700; color: #37474F;">${reportData.target_timeframe}</div>
                    </div>
                </div>
            </div>
        `;
        
        // 2. EXECUTIVE SUMMARY
        if (reportData.summary) {
            reportHTML += this.createReportSection('Executive Summary', 
                reportData.summary, '📋');
        }
        
        // 3. MARKET CONTEXT
        if (reportData.market_context) {
            reportHTML += this.createReportSection('Market Context', 
                reportData.market_context, '🌍');
        }
        
        // 4. TECHNICAL ANALYSIS (DETAILS)
        if (reportData.details) {
            reportHTML += this.createReportSection('Technical Analysis',
                reportData.details, '📊');
        }

        // 4.5 TRADING RECOMMENDATIONS
        if (reportData.trading_recommendations) {
            reportHTML += this.createReportSection('Trading Recommendations',
                reportData.trading_recommendations, '💡');
        }

        // 5. TREND DIRECTION
        if (reportData.trend_direction) {
            const trendColor = reportData.trend_direction.toLowerCase().includes('bull') ? '#2C8C6A' : 
                            reportData.trend_direction.toLowerCase().includes('bear') ? '#E54B4B' : '#F59E0B';
            reportHTML += `
                <div style="background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid ${trendColor};">
                    <h3 style="margin: 0 0 10px 0; color: #37474F; font-size: 1.1rem; display: flex; align-items: center; gap: 8px;">
                        <span>📈</span> Trend Direction
                    </h3>
                    <div style="font-size: 1.2rem; font-weight: 600; color: ${trendColor}; text-transform: capitalize;">
                        ${reportData.trend_direction}
                    </div>
                </div>
            `;
        }
        
        // 6. SUPPORT & RESISTANCE LEVELS
        if ((reportData.support_levels && reportData.support_levels.length > 0) || 
            (reportData.resistance_levels && reportData.resistance_levels.length > 0)) {
            
            let levelsHTML = '<div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">';
            
            // Support Levels
            if (reportData.support_levels && reportData.support_levels.length > 0) {
                levelsHTML += `
                    <div>
                        <h4 style="color: #2C8C6A; margin: 0 0 12px 0; font-size: 1rem;">Support Levels</h4>
                        <div style="display: flex; flex-direction: column; gap: 8px;">
                            ${reportData.support_levels.map((level, i) => {
                                const price = typeof level === 'object' ? (level.price || level.level) : level;
                                const strength = typeof level === 'object' ? level.strength : 'moderate';
                                return `
                                    <div style="padding: 10px; background: #e8f5f1; border-left: 3px solid #2C8C6A; border-radius: 4px;">
                                        <span style="font-size: 0.8rem; color: #90A4AE; text-transform: uppercase;">S${i + 1} (${strength})</span>
                                        <div style="font-size: 1.1rem; font-weight: 600; color: #2C8C6A;">₹${typeof price === 'number' ? price.toFixed(2) : price}</div>
                                    </div>
                                `;
                            }).join('')}
                        </div>
                    </div>
                `;
            }
            
            // Resistance Levels
            if (reportData.resistance_levels && reportData.resistance_levels.length > 0) {
                levelsHTML += `
                    <div>
                        <h4 style="color: #E54B4B; margin: 0 0 12px 0; font-size: 1rem;">Resistance Levels</h4>
                        <div style="display: flex; flex-direction: column; gap: 8px;">
                            ${reportData.resistance_levels.map((level, i) => {
                                const price = typeof level === 'object' ? (level.price || level.level) : level;
                                const strength = typeof level === 'object' ? level.strength : 'moderate';
                                return `
                                    <div style="padding: 10px; background: #fdecea; border-left: 3px solid #E54B4B; border-radius: 4px;">
                                        <span style="font-size: 0.8rem; color: #90A4AE; text-transform: uppercase;">R${i + 1} (${strength})</span>
                                        <div style="font-size: 1.1rem; font-weight: 600; color: #E54B4B;">₹${typeof price === 'number' ? price.toFixed(2) : price}</div>
                                    </div>
                                `;
                            }).join('')}
                        </div>
                    </div>
                `;
            }
            
            levelsHTML += '</div>';
            reportHTML += this.createReportSection('Key Price Levels', levelsHTML, '🎯', true);
        }
        
        // 7. ENTRY POINTS
        if (reportData.entry_points && reportData.entry_points.length > 0) {
            let entryHTML = '<div style="display: flex; flex-direction: column; gap: 12px;">';
            
            reportData.entry_points.forEach((entry, i) => {
                const entryPrice = typeof entry === 'object' ? (entry.price || entry.range_min) : entry;
                const entryType = typeof entry === 'object' ? entry.type : reportData.action;
                const confidence = typeof entry === 'object' ? entry.confidence : 'N/A';
                const color = entryType === 'buy' ? '#2C8C6A' : '#E54B4B';
                
                entryHTML += `
                    <div style="padding: 15px; background: white; border-left: 4px solid ${color}; border-radius: 6px;">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <div style="font-size: 0.85rem; color: #90A4AE; text-transform: uppercase; margin-bottom: 4px;">Entry Point ${i + 1}</div>
                                <div style="font-size: 1.3rem; font-weight: 700; color: ${color};">₹${typeof entryPrice === 'number' ? entryPrice.toFixed(2) : entryPrice}</div>
                            </div>
                            ${confidence !== 'N/A' ? `<div style="padding: 6px 12px; background: #f0f0f0; border-radius: 4px; font-size: 0.85rem; color: #666;">Confidence: ${confidence}%</div>` : ''}
                        </div>
                    </div>
                `;
            });
            
            entryHTML += '</div>';
            reportHTML += this.createReportSection('Entry Points', entryHTML, '🎯', true);
        }
        
        // 8. TARGETS
        if (reportData.targets && reportData.targets.length > 0) {
            let targetsHTML = '<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px;">';
            
            reportData.targets.forEach((target, i) => {
                const targetPrice = typeof target === 'object' ? target.price : target;
                const probability = typeof target === 'object' ? target.probability : 'N/A';
                
                targetsHTML += `
                    <div style="padding: 15px; background: #e8f2f7; border-left: 3px solid #4A6C8C; border-radius: 6px; text-align: center;">
                        <div style="font-size: 0.8rem; color: #90A4AE; text-transform: uppercase; margin-bottom: 6px;">Target ${i + 1}</div>
                        <div style="font-size: 1.4rem; font-weight: 700; color: #4A6C8C;">₹${typeof targetPrice === 'number' ? targetPrice.toFixed(2) : targetPrice}</div>
                        ${probability !== 'N/A' ? `<div style="font-size: 0.8rem; color: #666; margin-top: 4px;">${probability}% probability</div>` : ''}
                    </div>
                `;
            });
            
            targetsHTML += '</div>';
            reportHTML += this.createReportSection('Price Targets', targetsHTML, '🎯', true);
        }
        
        // 9. STOP LOSS
        let stopLossPrice = null;
        if (reportData.stop_loss) {
            if (typeof reportData.stop_loss === 'object') {
                // Try multiple possible field names
                stopLossPrice = reportData.stop_loss.price ||
                                reportData.stop_loss.level ||
                                reportData.stop_loss.stop_loss ||
                                Object.values(reportData.stop_loss)[0]; // Fallback to first value
            } else if (typeof reportData.stop_loss === 'number') {
                stopLossPrice = reportData.stop_loss;
            } else if (typeof reportData.stop_loss === 'string' && !isNaN(reportData.stop_loss)) {
                stopLossPrice = parseFloat(reportData.stop_loss);
            }
        }

        if (stopLossPrice) {
            reportHTML += `
                <div style="background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #E54B4B;">
                    <h3 style="margin: 0 0 10px 0; color: #37474F; font-size: 1.1rem; display: flex; align-items: center; gap: 8px;">
                        <span>🛑</span> Stop Loss
                    </h3>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #E54B4B;">
                        ₹${typeof stopLossPrice === 'number' ? stopLossPrice.toFixed(2) : stopLossPrice}
                    </div>
                </div>
            `;
        }
        
        // 10. PATTERNS
        if (reportData.patterns && reportData.patterns.length > 0) {
            let patternsHTML = '<div style="display: flex; flex-direction: column; gap: 12px;">';
            
            reportData.patterns.forEach((pattern) => {
                const patternName = typeof pattern === 'object' ? pattern.pattern_name : pattern;
                const confidence = typeof pattern === 'object' ? pattern.confidence : 'N/A';
                
                patternsHTML += `
                    <div style="padding: 15px; background: white; border-left: 3px solid #4A6C8C; border-radius: 6px;">
                        <div style="font-weight: 600; color: #37474F; font-size: 1rem; margin-bottom: 4px; text-transform: capitalize;">
                            ${patternName.replace(/_/g, ' ')}
                        </div>
                        ${confidence !== 'N/A' ? `<div style="font-size: 0.85rem; color: #666;">Confidence: ${confidence}%</div>` : ''}
                    </div>
                `;
            });
            
            patternsHTML += '</div>';
            reportHTML += this.createReportSection('Detected Patterns', patternsHTML, '📐', true);
        }
        
        // 11. METADATA FOOTER
        reportHTML += `
            <div style="background: #f5f5f5; padding: 15px 20px; border-radius: 8px; margin-top: 30px; border-top: 2px solid #CFD8DC;">
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; font-size: 0.85rem; color: #666;">
                    ${reportData.data_points ? `<div><strong>Data Points:</strong> ${reportData.data_points}</div>` : ''}
                    ${reportData.indicators_used && reportData.indicators_used.length > 0 ?
                        `<div><strong>Indicators:</strong> ${reportData.indicators_used.join(', ')}</div>` : ''}
                    <div><strong>Report Generated:</strong> ${new Date().toLocaleString()}</div>
                </div>
            </div>
        `;
        
        contentArea.innerHTML = reportHTML;
        
        // Assemble modal
        reportContainer.appendChild(header);
        reportContainer.appendChild(contentArea);
        modal.appendChild(reportContainer);
        
        // Close button handler
        setTimeout(() => {
            const closeBtn = document.getElementById('modal-close-btn');
            if (closeBtn) {
                closeBtn.onmouseover = () => closeBtn.style.background = 'rgba(255,255,255,0.3)';
                closeBtn.onmouseout = () => closeBtn.style.background = 'rgba(255,255,255,0.2)';
                closeBtn.onclick = () => modal.style.display = 'none';
            }
        }, 0);
        
        return modal;
    }


    createReportSection(title, content, icon = '', isHTML = false) {
        return `
            <div style="background: white; padding: 20px 25px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
                <h3 style="margin: 0 0 15px 0; color: #37474F; font-size: 1.3rem; border-bottom: 2px solid #ECEFF1; padding-bottom: 10px;">
                    ${icon} ${title}
                </h3>
                <div style="color: #37474F; line-height: 1.7; font-size: 0.95rem;">
                    ${isHTML ? content : this.formatMessage(content)}
                </div>
            </div>
        `;
    }



    createTabPane(name, content, visible = false) {
        const pane = document.createElement('div');
        pane.className = 'tab-pane';
        pane.dataset.pane = name;
        pane.style.cssText = `
            display: ${visible ? 'block' : 'none'};
            line-height: 1.6;
            color: #37474F;
            font-size: 0.95rem;
        `;
        
        // Format content if it's an object
        let formattedContent = content;
        if (typeof content === 'object') {
            formattedContent = JSON.stringify(content, null, 2);
        }
        
        pane.innerHTML = `<div style="white-space: pre-wrap; word-wrap: break-word;">${this.formatMessage(formattedContent)}</div>`;
        
        return pane;
    }

}

// Initialize
window.aladinApp = new ALADINStreamingClient();

// Debug utilities
window.ALADIN = {
    app: window.aladinApp,
    testChart: (symbol = 'TCS') => window.aladinApp.loadAnalysisVisualization(symbol),
    testStreaming: (symbol = 'TCS') => {
        window.aladinApp.chatInput.value = `analyze ${symbol}`;
        window.aladinApp.sendMessage();
    },
    fetchChart: async (symbol) => await window.aladinApp.fetchChartImage(symbol),
    zoomIn: () => window.aladinApp.zoomChart(1.2),
    zoomOut: () => window.aladinApp.zoomChart(0.8),
    reset: () => window.aladinApp.resetChartView()
};

console.log('🚀 ALADIN v2.1 loaded - Image-based charts with zoom/pan');
console.log('💡 Test: ALADIN.testChart("TCS")');
// Manufacturing Analytics AI - Main JavaScript

// Global variables
let currentMachine = '';
let currentCharts = [];
let currentResponse = '';
let chatHistory = [];

// Initialize the application
function initializeApp() {
    console.log('Initializing Manufacturing Analytics AI...');
    
    // Setup event listeners
    setupEventListeners();s
    
    // Load initial data
    loadMachines();
    
    // Setup chat
    initializeChat();
    
    console.log('Application initialized successfully');
}

// Setup all event listeners
function setupEventListeners() {
    // Machine selection
    document.getElementById('machineSelect').addEventListener('change', handleMachineSelection);
    
    // Chat functionality
    document.getElementById('sendChat').addEventListener('click', sendChatMessage);
    document.getElementById('chatInput').addEventListener('keypress', function(e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            sendChatMessage();
        }
    });
    
    // Example queries
    document.querySelectorAll('.example-query').forEach(button => {
        button.addEventListener('click', function() {
            const query = this.getAttribute('data-query');
            document.getElementById('chatInput').value = query;
            sendChatMessage();
        });
    });
    
    // Refresh files
    document.getElementById('refreshFiles').addEventListener('click', function() {
        if (currentMachine) {
            loadMachineFiles(currentMachine);
        }
    });
    
    // Export functionality
    document.getElementById('exportPDF').addEventListener('click', exportToPDF);
    document.getElementById('exportImages').addEventListener('click', exportChartsAsImages);
}

// Load available machines
function loadMachines() {
    fetch('/api/machines')
        .then(response => response.json())
        .then(data => {
            if (data.machines) {
                updateMachineSelect(data.machines);
                updateDataStatus('Machines loaded');
            } else {
                console.error('Error loading machines:', data.error);
                updateDataStatus('Error loading machines', 'danger');
            }
        })
        .catch(error => {
            console.error('Error fetching machines:', error);
            updateDataStatus('Connection error', 'danger');
        });
}

// Update machine select dropdown
function updateMachineSelect(machines) {
    const select = document.getElementById('machineSelect');
    
    // Clear existing options (except first)
    while (select.children.length > 1) {
        select.removeChild(select.lastChild);
    }
    
    // Add machine options
    machines.forEach(machine => {
        const option = document.createElement('option');
        option.value = machine;
        option.textContent = machine;
        select.appendChild(option);
    });
    
    console.log(`Loaded ${machines.length} machines`);
}

// Handle machine selection
function handleMachineSelection() {
    const select = document.getElementById('machineSelect');
    const selectedMachine = select.value;
    
    if (selectedMachine) {
        currentMachine = selectedMachine;
        loadMachineFiles(selectedMachine);
        enableChat();
        updateDataStatus(`Machine ${selectedMachine} selected`, 'success');
    } else {
        currentMachine = '';
        disableChat();
        hideMachineInfo();
        updateDataStatus('No machine selected');
    }
}

// Load files for selected machine
function loadMachineFiles(machine) {
    fetch(`/api/machine-files/${machine}`)
        .then(response => response.json())
        .then(data => {
            if (data.files) {
                showMachineInfo(data.files);
                updateDataStatus(`${data.files.length} files found`, 'success');
            } else {
                console.error('Error loading files:', data.error);
                updateDataStatus('Error loading files', 'warning');
            }
        })
        .catch(error => {
            console.error('Error fetching machine files:', error);
            updateDataStatus('Connection error', 'danger');
        });
}

// Show machine information
function showMachineInfo(files) {
    const infoDiv = document.getElementById('machineInfo');
    const filesDiv = document.getElementById('machineFiles');
    
    if (files.length > 0) {
        let filesHtml = '<strong>Available Files:</strong><br>';
        files.forEach(file => {
            const size = (file.size / 1024).toFixed(1);
            filesHtml += `• ${file.filename} (${size} KB)<br>`;
        });
        filesDiv.innerHTML = filesHtml;
    } else {
        filesDiv.innerHTML = '<strong>No CSV files found</strong><br>Please add data files to the machine folder.';
    }
    
    infoDiv.classList.remove('d-none');
}

// Hide machine information
function hideMachineInfo() {
    document.getElementById('machineInfo').classList.add('d-none');
}

// Initialize chat
function initializeChat() {
    // Add welcome message
    addChatMessage('ai', 'Welcome to Manufacturing Analytics AI! Select a machine from the sidebar and ask me anything about your manufacturing data. I can provide summaries, comparisons, trends, and generate charts for you.');
}

// Enable chat functionality
function enableChat() {
    document.getElementById('chatInput').disabled = false;
    document.getElementById('sendChat').disabled = false;
    document.getElementById('chatInput').placeholder = 'Ask me about your manufacturing data...';
}

// Disable chat functionality
function disableChat() {
    document.getElementById('chatInput').disabled = true;
    document.getElementById('sendChat').disabled = true;
    document.getElementById('chatInput').placeholder = 'Please select a machine first...';
}

// Send chat message
function sendChatMessage() {
    const input = document.getElementById('chatInput');
    const query = input.value.trim();
    
    if (!query) {
        return;
    }
    
    if (!currentMachine) {
        alert('Please select a machine first!');
        return;
    }
    
    // Add user message to chat
    addChatMessage('user', query);
    
    // Clear input
    input.value = '';
    
    // Show loading
    showLoadingModal();
    updateAIStatus('Processing...', 'warning');
    
    // Send request to backend
    fetch('/api/chat', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            query: query,
            machine: currentMachine
        })
    })
    .then(response => response.json())
    .then(data => {
        hideLoadingModal();
        handleChatResponse(data);
    })
    .catch(error => {
        hideLoadingModal();
        console.error('Error sending chat message:', error);
        addChatMessage('ai', 'Sorry, I encountered an error while processing your request. Please try again.');
        updateAIStatus('Error', 'danger');
    });
}

// Handle chat response
function handleChatResponse(data) {
    if (data.type === 'error') {
        addChatMessage('ai', data.response, 'error');
        updateAIStatus('Error', 'danger');
        return;
    }
    
    // Add file-specific indicator if applicable
    let responseText = data.response;
    if (data.file_specific && data.loaded_file) {
        responseText = `**Analyzing: ${data.loaded_file}**\n\n${responseText}`;
    }
    
    // Add AI response
    addChatMessage('ai', responseText);
    currentResponse = responseText;
    
    // Display charts if available
    if (data.charts && data.charts.length > 0) {
        currentCharts = data.charts;
        displayCharts(data.charts);
        updateChartCount(data.charts.length);
        
        // Show success message for chart generation
        if (data.file_specific) {
            updateDataStatus(`Charts generated for ${data.loaded_file}`, 'success');
        }
    } else {
        updateChartCount(0);
    }
    
    updateAIStatus('Ready', 'success');
}

// Add message to chat
function addChatMessage(sender, message, type = '') {
    const chatContainer = document.getElementById('chatMessages');
    const messageDiv = document.createElement('div');
    
    messageDiv.className = `chat-message ${sender} ${type} fade-in`;
    
    const timestamp = new Date().toLocaleTimeString();
    
    let messageHtml = `
        <div class="message-content">${formatMessage(message)}</div>
        <div class="message-time">${timestamp}</div>
    `;
    
    if (sender === 'ai' && currentCharts.length > 0) {
        messageHtml += `
            <div class="export-controls">
                <button class="btn btn-outline-primary btn-sm" onclick="showExportModal()">
                    <i class="fas fa-download me-1"></i>Export
                </button>
            </div>
        `;
    }
    
    messageDiv.innerHTML = messageHtml;
    chatContainer.appendChild(messageDiv);
    
    // Scroll to bottom
    chatContainer.scrollTop = chatContainer.scrollHeight;
    
    // Store in history
    chatHistory.push({
        sender: sender,
        message: message,
        timestamp: timestamp,
        type: type
    });
}

// Format message content
function formatMessage(message) {
    // Escape HTML tags to prevent XSS
    message = message.replace(/</g, '&lt;').replace(/>/g, '&gt;');

    // Convert triple backtick code blocks (```code```)
    message = message.replace(/```([^`]+)```/gs, '<pre><code>$1</code></pre>');

    // Convert inline code blocks (`code`)
    message = message.replace(/`([^`]+)`/g, '<code>$1</code>');

    // Convert bold (**bold**)
    message = message.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');

    // Convert headings (#, ##, ###)
    message = message.replace(/^### (.*)$/gm, '<h5>$1</h5>');
    message = message.replace(/^## (.*)$/gm, '<h4>$1</h4>');
    message = message.replace(/^# (.*)$/gm, '<h3>$1</h3>');

    // Convert numbered lists (1. 2. etc.)
    if (/\d+\.\s/.test(message)) {
        const lines = message.split('<br>');
        let inList = false;
        let formattedLines = [];

        lines.forEach(line => {
            if (/^\d+\.\s/.test(line.trim())) {
                if (!inList) {
                    formattedLines.push('<ol>');
                    inList = true;
                }
                const listItem = line.replace(/^\d+\.\s/, '').trim();
                formattedLines.push(`<li>${listItem}</li>`);
            } else {
                if (inList) {
                    formattedLines.push('</ol>');
                    inList = false;
                }
                formattedLines.push(line);
            }
        });

        if (inList) {
            formattedLines.push('</ol>');
        }

        message = formattedLines.join('<br>');
    }

    // Convert bullet lists (-, *, •)
    if (message.includes('•') || message.includes('- ') || message.includes('* ')) {
        const lines = message.split('<br>');
        let inList = false;
        let formattedLines = [];

        lines.forEach(line => {
            if (line.trim().match(/^[-*•]\s+/)) {
                if (!inList) {
                    formattedLines.push('<ul>');
                    inList = true;
                }
                const listItem = line.replace(/^[-*•]\s+/, '').trim();
                formattedLines.push(`<li>${listItem}</li>`);
            } else {
                if (inList) {
                    formattedLines.push('</ul>');
                    inList = false;
                }
                formattedLines.push(line);
            }
        });

        if (inList) {
            formattedLines.push('</ul>');
        }

        message = formattedLines.join('<br>');
    }

    // Convert markdown-style tables
    if (message.includes('|') && message.includes('---')) {
        const lines = message.split('<br>');
        let tableHtml = '';
        let inTable = false;

        lines.forEach(line => {
            if (line.includes('|')) {
                const cells = line.split('|').map(cell => cell.trim()).filter(cell => cell.length);
                if (!inTable) {
                    tableHtml += '<table class="table table-dark table-bordered table-sm mt-2"><thead><tr>';
                    cells.forEach(cell => tableHtml += `<th>${cell}</th>`);
                    tableHtml += '</tr></thead><tbody>';
                    inTable = true;
                } else if (!line.includes('---')) {
                    tableHtml += '<tr>';
                    cells.forEach(cell => tableHtml += `<td>${cell}</td>`);
                    tableHtml += '</tr>';
                }
            } else if (inTable) {
                tableHtml += '</tbody></table>';
                inTable = false;
                tableHtml += `<br>${line}`;
            } else {
                tableHtml += `${line}<br>`;
            }
        });

        if (inTable) {
            tableHtml += '</tbody></table>';
        }

        message = tableHtml;
    }

    // Convert line breaks last
    message = message.replace(/\n/g, '<br>');

    return message;
}

// Display charts
function displayCharts(charts) {
    const container = document.getElementById('chartsContainer');
    container.innerHTML = '';
    
    charts.forEach((chart, index) => {
        const chartDiv = document.createElement('div');
        chartDiv.className = 'chart-container fade-in';
        
        chartDiv.innerHTML = `
            <div class="chart-card">
                <div class="chart-header">
                    <span>${chart.title}</span>
                    <div class="chart-actions">
                        <button class="btn btn-light btn-sm" onclick="zoomChart(${index})" title="View Full Size">
                            <i class="fas fa-search-plus"></i>
                        </button>
                        <button class="btn btn-light btn-sm" onclick="downloadChart(${index})" title="Download">
                            <i class="fas fa-download"></i>
                        </button>
                    </div>
                </div>
                <div class="chart-body">
                    <img src="data:image/png;base64,${chart.image}" class="chart-image" alt="${chart.title}" onclick="zoomChart(${index})" style="cursor: pointer;">
                    ${chart.description ? `<div class="chart-description">${chart.description}</div>` : ''}
                </div>
            </div>
        `;
        
        container.appendChild(chartDiv);
    });
}

// Download individual chart
function downloadChart(index) {
    if (currentCharts[index]) {
        const chart = currentCharts[index];
        const link = document.createElement('a');
        link.href = `data:image/png;base64,${chart.image}`;
        link.download = `${chart.title.replace(/[^a-z0-9]/gi, '_').toLowerCase()}.png`;
        link.click();
    }
}

// Show export modal
function showExportModal() {
    const modal = new bootstrap.Modal(document.getElementById('exportModal'));
    modal.show();
}

// Export to PDF
function exportToPDF() {
    if (!currentResponse && currentCharts.length === 0) {
        alert('No content to export!');
        return;
    }
    
    showLoadingModal();
    
    fetch('/api/export-pdf', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            content: currentResponse,
            charts: currentCharts
        })
    })
    .then(response => {
        if (response.ok) {
            return response.blob();
        }
        throw new Error('Export failed');
    })
    .then(blob => {
        hideLoadingModal();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `manufacturing_report_${new Date().toISOString().split('T')[0]}.pdf`;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        
        // Hide export modal
        const modal = bootstrap.Modal.getInstance(document.getElementById('exportModal'));
        modal.hide();
    })
    .catch(error => {
        hideLoadingModal();
        console.error('Error exporting PDF:', error);
        alert('Error exporting PDF. Please try again.');
    });
}

// Export charts as images
function exportChartsAsImages() {
    if (currentCharts.length === 0) {
        alert('No charts to export!');
        return;
    }
    
    currentCharts.forEach((chart, index) => {
        setTimeout(() => {
            downloadChart(index);
        }, index * 100); // Small delay between downloads
    });
    
    // Hide export modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('exportModal'));
    modal.hide();
}

// Show loading modal
function showLoadingModal() {
    const modal = new bootstrap.Modal(document.getElementById('loadingModal'));
    modal.show();
}

// Hide loading modal
function hideLoadingModal() {
    const modal = bootstrap.Modal.getInstance(document.getElementById('loadingModal'));
    if (modal) {
        modal.hide();
    }
}

// Update status indicators
function updateDataStatus(message, type = 'secondary') {
    const statusElement = document.getElementById('dataStatus');
    statusElement.textContent = message;
    statusElement.className = `badge bg-${type}`;
}

function updateAIStatus(message, type = 'success') {
    const statusElement = document.getElementById('aiStatus');
    statusElement.textContent = message;
    statusElement.className = `badge bg-${type}`;
}

function updateChartCount(count) {
    const countElement = document.getElementById('chartCount');
    countElement.textContent = count;
    countElement.className = count > 0 ? 'badge bg-info' : 'badge bg-secondary';
}

// Utility functions
function formatNumber(num) {
    return new Intl.NumberFormat().format(num);
}

function formatCurrency(amount) {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD'
    }).format(amount);
}

function formatDate(dateString) {
    return new Date(dateString).toLocaleDateString();
}

// Error handling
window.addEventListener('error', function(e) {
    console.error('JavaScript error:', e.error);
    updateAIStatus('Error', 'danger');
});

// Handle fetch errors
window.addEventListener('unhandledrejection', function(e) {
    console.error('Unhandled promise rejection:', e.reason);
    updateAIStatus('Connection Error', 'danger');
});

// Zoom chart functionality
function zoomChart(index) {
    if (currentCharts[index]) {
        const chart = currentCharts[index];
        
        // Create modal for zoomed view
        const modal = document.createElement('div');
        modal.className = 'modal fade';
        modal.id = 'chartZoomModal';
        modal.setAttribute('tabindex', '-1');
        
        modal.innerHTML = `
            <div class="modal-dialog modal-xl modal-dialog-centered">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">${chart.title} - Full View</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body text-center">
                        <img src="data:image/png;base64,${chart.image}" 
                             class="img-fluid" 
                             alt="${chart.title}"
                             style="max-width: 100%; height: auto; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1);">
                        ${chart.description ? `<div class="mt-3 text-muted">${chart.description}</div>` : ''}
                    </div>
                    <div class="modal-footer">
                        <button class="btn btn-primary" onclick="downloadChart(${index})">
                            <i class="fas fa-download me-1"></i>Download Chart
                        </button>
                        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
                    </div>
                </div>
            </div>
        `;
        
        // Remove existing modal if any
        const existingModal = document.getElementById('chartZoomModal');
        if (existingModal) {
            existingModal.remove();
        }
        
        // Add modal to body and show
        document.body.appendChild(modal);
        const bootstrapModal = new bootstrap.Modal(modal);
        bootstrapModal.show();
        
        // Clean up when modal is hidden
        modal.addEventListener('hidden.bs.modal', function() {
            modal.remove();
        });
    }
}

// Auto-refresh machines every 30 seconds
setInterval(function() {
    if (!document.hidden) {
        loadMachines();
    }
}, 30000);

console.log('Manufacturing Analytics AI JavaScript loaded successfully');

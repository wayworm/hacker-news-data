function addKeyword(keyword) {
    const input = document.getElementById('keywords');
    const current = input.value.trim();
    
    if (current === '') {
        input.value = keyword;
    } else {
        const keywords = current.split(',').map(k => k.trim());
        if (!keywords.includes(keyword)) {
            input.value = current + ', ' + keyword;
        }
    }
}

document.getElementById('analysisForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const formData = {
        keywords: document.getElementById('keywords').value,
        timeBin: document.getElementById('timeBin').value,
        rolling: document.getElementById('rolling').value,
        refresh: document.getElementById('refresh').checked
    };
    
    // Show loading
    document.getElementById('loading').style.display = 'block';
    document.getElementById('result').style.display = 'none';
    document.getElementById('submitBtn').disabled = true;
    
    try {
        const response = await fetch('/analyse', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });
        
        const data = await response.json();
        
        if (data.success) {
            let statusHTML = '<h3>Analysis Complete</h3>';
            data.results.forEach(r => {
                if (r.status === 'success') {
                    statusHTML += `<div class="status-item"><span class="success">✓</span> ${r.keyword}: ${r.points} data points</div>`;
                } else {
                    statusHTML += `<div class="status-item"><span class="error">✗</span> ${r.keyword}: No data found</div>`;
                }
            });
            
            document.getElementById('status').innerHTML = statusHTML;
            document.getElementById('resultImage').src = data.image + '?t=' + Date.now();
            document.getElementById('result').style.display = 'block';
        } else {
            alert('Error: ' + data.error);
        }
    } catch (error) {
        alert('Error: ' + error.message);
    } finally {
        document.getElementById('loading').style.display = 'none';
        document.getElementById('submitBtn').disabled = false;
    }
});

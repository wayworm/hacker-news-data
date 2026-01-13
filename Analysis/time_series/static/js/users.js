document.getElementById('topUsersForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const submitBtn = document.getElementById('submitBtn');
    const loading = document.getElementById('loading');
    const result = document.getElementById('result');
    const visualizations = document.getElementById('visualizations');
    const tableContainer = document.getElementById('tableContainer');
    
    submitBtn.disabled = true;
    loading.style.display = 'block';
    result.style.display = 'none';
    
    const formData = {
        itemType: document.getElementById('itemType').value,
        limit: parseInt(document.getElementById('limit').value),
        refresh: document.getElementById('refresh').checked
    };
    
    try {
        const response = await fetch('/users', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(formData)
        });
        
        const data = await response.json();
        
        if (data.success) {
            visualizations.innerHTML = data.images.map(img => 
                `<img src="${img}" alt="Visualization" style="max-width: 100%; margin: 1rem 0; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">`
            ).join('');
            
            tableContainer.innerHTML = `
                <h3 style="margin-top: 2rem;">Top ${Math.min(20, data.users.length)} Users by Cumulative Score</h3>
                <div style="overflow-x: auto;">
                    <table class="users-table">
                        <thead>
                            <tr>
                                <th>Rank</th>
                                <th>Username</th>
                                <th>Total Posts</th>
                                <th>Cumulative Score</th>
                                <th>Avg Score</th>
                                <th>Top Post</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${data.users.slice(0, 20).map((user, idx) => `
                                <tr>
                                    <td>${idx + 1}</td>
                                    <td><a href="https://news.ycombinator.com/user?id=${user.username}" target="_blank">${user.username}</a></td>
                                    <td>${user.total_posts.toLocaleString()}</td>
                                    <td>${user.cumulative_score.toLocaleString()}</td>
                                    <td>${user.avg_score}</td>
                                    <td>${user.top_post_score.toLocaleString()}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            `;
            
            result.style.display = 'block';
        } else {
            alert('Error: ' + data.error);
        }
    } catch (error) {
        alert('Failed to fetch data: ' + error.message);
    } finally {
        loading.style.display = 'none';
        submitBtn.disabled = false;
    }
});
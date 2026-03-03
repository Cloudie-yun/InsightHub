const authModal = document.getElementById("auth-modal");
const loginForm = document.getElementById("auth-login");
const signupForm = document.getElementById("auth-signup");
const forgotForm = document.getElementById("auth-forgot");
const authTitle = document.getElementById("auth-title");
const authSubtitle = document.getElementById("auth-subtitle");
const userSection = document.getElementById("user-section");
const authSection = document.getElementById("auth-section");
const sidebarUserAvatarInitial = document.getElementById("sidebar-user-avatar-initial");
const sidebarUserName = document.getElementById("sidebar-user-name");
const sidebarUserEmail = document.getElementById("sidebar-user-email");
const emailVerifyReminder = document.getElementById("email-verify-reminder");
const emailVerifyBadge = document.getElementById("email-verify-badge");

// ========== CUSTOM PLACEHOLDER MANAGEMENT ==========
const initCustomPlaceholders = () => {
    const inputs = document.querySelectorAll('#auth-modal input[type="text"], #auth-modal input[type="email"], #auth-modal input[type="password"]');
    
    inputs.forEach(input => {
        const container = input.parentElement;
        const placeholder = container.querySelector('.custom-placeholder');
        
        if (!placeholder) return;
        
        const updatePlaceholder = () => {
            if (input.value.length > 0) {
                placeholder.style.opacity = '0';
                placeholder.style.visibility = 'hidden';
            } else {
                placeholder.style.opacity = '1';
                placeholder.style.visibility = 'visible';
            }
        };
        
        input.addEventListener('focus', () => {
            placeholder.style.opacity = '0';
            placeholder.style.visibility = 'hidden';
        });
        
        input.addEventListener('blur', () => {
            if (input.value.length === 0) {
                placeholder.style.opacity = '1';
                placeholder.style.visibility = 'visible';
            }
        });
        
        input.addEventListener('input', updatePlaceholder);
        updatePlaceholder();
    });
};

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCustomPlaceholders);
} else {
    initCustomPlaceholders();
}

// ========== FORM UTILITIES ==========
const clearFormErrors = (formId) => {
    document.querySelectorAll(`#${formId} span[id$='-error']`).forEach((el) => {
        el.classList.add("hidden");
    });
    document.querySelectorAll(`#${formId} input`).forEach((input) => {
        input.classList.remove("auth-input-error");
    });
};

const resetAuthFormState = (formId) => {
    const form = document.getElementById(formId);
    if (!form) return;
    form.reset();
    clearFormErrors(formId);
    
    form.querySelectorAll("input[type='text']").forEach((input) => {
        if (input.id.toLowerCase().includes("password")) {
            input.type = "password";
        }
    });
    
    form.querySelectorAll('.custom-placeholder').forEach(placeholder => {
        placeholder.style.opacity = '1';
        placeholder.style.visibility = 'visible';
    });
};

const resetPasswordToggleIcons = () => {
    document.querySelectorAll(".auth-password-toggle").forEach((button) => {
        button.innerHTML = '<i class="fa-solid fa-eye"></i>';
        button.setAttribute("aria-label", "Show password");
    });
};

const resetAuthForms = () => {
    resetAuthFormState("auth-login");
    resetAuthFormState("auth-signup");
    resetAuthFormState("auth-forgot");
    resetPasswordToggleIcons();
    
    // Reset password strength dropdown
    const dropdown = document.getElementById('password-strength-dropdown');
    if (dropdown) {
        dropdown.classList.add('hidden');
        resetPasswordStrengthIndicators();
    }

    resetForgotPasswordStep();
};

// ========== AUTH MODAL CONTROLS ==========
const setAuthMode = (mode) => {
    resetAuthForms();
    const isLogin = mode === "login";
    const isSignup = mode === "signup";
    const isForgot = mode === "forgot";

    loginForm.classList.toggle("hidden", !isLogin);
    signupForm.classList.toggle("hidden", !isSignup);
    forgotForm.classList.toggle("hidden", !isForgot);

    if (isLogin) {
        authTitle.textContent = "Welcome Back";
        authSubtitle.textContent = "Log in to continue";
    } else if (isSignup) {
        authTitle.textContent = "Create Your Account";
        authSubtitle.textContent = "Sign up in seconds";
    } else {
        authTitle.textContent = "Forgot Password";
        authSubtitle.textContent = "Enter your email to verify your account";
    }

    setTimeout(initCustomPlaceholders, 10);
};

const openAuth = (mode) => {
    setAuthMode(mode);
    authModal.classList.remove("hidden");
    authModal.classList.add("flex");
};

const closeAuth = () => {
    resetAuthForms();
    authModal.classList.add("hidden");
    authModal.classList.remove("flex");
};

document.querySelectorAll("[data-auth-open]").forEach((el) => {
    el.addEventListener("click", () => openAuth(el.dataset.authOpen));
});

document.querySelectorAll("[data-auth-close]").forEach((el) => {
    el.addEventListener("click", closeAuth);
});

document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeAuth();
});

// ========== VALIDATION ==========
const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const strongPasswordRegex = /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z\d]).{8,}$/;

const showError = (input, errorId, show) => {
    const error = document.getElementById(errorId);
    if (!error) return;
    error.classList.toggle("hidden", !show);
    if (input) {
        input.classList.toggle("auth-input-error", show);
    }
};

// ========== FORGOT PASSWORD FLOW ==========
const forgotEmailInput = document.getElementById("forgot-email");
const forgotCodeInput = document.getElementById("forgot-code");
const forgotCodeStep = document.getElementById("forgot-code-step");
const forgotSubmitBtn = document.getElementById("forgot-submit-btn");
const codeRegex = /^\d{6}$/;

const setAuthUiState = (user) => {
    const isLoggedIn = Boolean(user && user.user_id);
    userSection.classList.toggle("hidden", !isLoggedIn);
    authSection.classList.toggle("hidden", isLoggedIn);

    if (!isLoggedIn) {
        if (emailVerifyReminder) emailVerifyReminder.classList.add("hidden");
        if (emailVerifyBadge) emailVerifyBadge.classList.add("hidden");
        return;
    }

    const username = (user.username || "").trim();
    const email = (user.email || "").trim();
    const initial = username ? username.charAt(0).toUpperCase() : "U";
    const isVerified = Boolean(user.email_verified);

    if (sidebarUserAvatarInitial) sidebarUserAvatarInitial.textContent = initial;
    sidebarUserName.textContent = username || "User";
    sidebarUserEmail.textContent = email;
    if (emailVerifyReminder) emailVerifyReminder.classList.toggle("hidden", isVerified);
    if (emailVerifyBadge) emailVerifyBadge.classList.toggle("hidden", isVerified);
};

const resetForgotPasswordStep = () => {
    if (!forgotCodeStep || !forgotSubmitBtn) return;
    forgotCodeStep.classList.add("hidden");
    forgotSubmitBtn.textContent = "Send Verification Code";

    if (forgotCodeInput) {
        forgotCodeInput.value = "";
        hideAllErrorsForInput(forgotCodeInput);
    }
};

const showForgotPasswordCodeStep = () => {
    if (!forgotCodeStep || !forgotSubmitBtn) return;
    forgotCodeStep.classList.remove("hidden");
    forgotSubmitBtn.textContent = "Verify Code";
    authSubtitle.textContent = "Enter the 6-digit code sent to your email";
    setTimeout(initCustomPlaceholders, 10);
};

const hideAllErrorsForInput = (input) => {
    const inputId = input.id;
    document.querySelectorAll(`span[id^="${inputId}-"]`).forEach(error => {
        error.classList.add("hidden");
    });
    input.classList.remove("auth-input-error");
};

// ========== PASSWORD STRENGTH INDICATOR ==========
const passwordStrengthDropdown = document.getElementById('password-strength-dropdown');
const signupPasswordInput = document.getElementById('signup-password');

const updatePasswordStrengthIndicator = (requirement, isValid) => {
    const element = document.getElementById(requirement);
    if (!element) return;
    
    const icon = element.querySelector('i');
    const text = element.querySelector('span');
    
    if (isValid) {
        icon.classList.remove('fa-xmark', 'text-red-500');
        icon.classList.add('fa-check', 'text-green-500');
        text.classList.remove('text-slate-500');
        text.classList.add('text-green-600');
    } else {
        icon.classList.remove('fa-check', 'text-green-500');
        icon.classList.add('fa-xmark', 'text-red-500');
        text.classList.remove('text-green-600');
        text.classList.add('text-slate-500');
    }
};

const resetPasswordStrengthIndicators = () => {
    ['pwd-length', 'pwd-uppercase', 'pwd-lowercase', 'pwd-number', 'pwd-special'].forEach(req => {
        updatePasswordStrengthIndicator(req, false);
    });
};

const checkPasswordStrength = (password) => {
    const hasLength = password.length >= 8;
    const hasUppercase = /[A-Z]/.test(password);
    const hasLowercase = /[a-z]/.test(password);
    const hasNumber = /\d/.test(password);
    const hasSpecial = /[^A-Za-z\d]/.test(password);
    
    updatePasswordStrengthIndicator('pwd-length', hasLength);
    updatePasswordStrengthIndicator('pwd-uppercase', hasUppercase);
    updatePasswordStrengthIndicator('pwd-lowercase', hasLowercase);
    updatePasswordStrengthIndicator('pwd-number', hasNumber);
    updatePasswordStrengthIndicator('pwd-special', hasSpecial);
    
    return hasLength && hasUppercase && hasLowercase && hasNumber && hasSpecial;
};

// Show dropdown when password field is focused
signupPasswordInput.addEventListener('focus', () => {
    passwordStrengthDropdown.classList.remove('hidden');
    hideAllErrorsForInput(signupPasswordInput);
});

// Hide dropdown when all requirements are met
let hideDropdownTimeout;
signupPasswordInput.addEventListener('blur', () => {
    // Delay hiding to allow user to see final state
    hideDropdownTimeout = setTimeout(() => {
        passwordStrengthDropdown.classList.add('hidden');
    }, 200);
});

// Cancel hide if user focuses back
signupPasswordInput.addEventListener('focus', () => {
    clearTimeout(hideDropdownTimeout);
});

// ========== PASSWORD TOGGLE ==========
document.querySelectorAll(".auth-password-toggle").forEach((button) => {
    button.addEventListener("click", () => {
        const input = document.getElementById(button.dataset.target);
        if (!input) return;

        const isHidden = input.type === "password";
        input.type = isHidden ? "text" : "password";
        button.innerHTML = isHidden
            ? '<i class="fa-solid fa-eye-slash"></i>'
            : '<i class="fa-solid fa-eye"></i>';
        button.setAttribute("aria-label", isHidden ? "Hide password" : "Show password");
    });
});

// ========== REAL-TIME VALIDATION - LOGIN ==========
const loginEmailInput = document.getElementById("login-email");
const loginPasswordInput = document.getElementById("login-password");

loginEmailInput.addEventListener('focus', () => {
    hideAllErrorsForInput(loginEmailInput);
});

loginPasswordInput.addEventListener('focus', () => {
    hideAllErrorsForInput(loginPasswordInput);
});

loginEmailInput.addEventListener('input', () => {
    const email = loginEmailInput.value.trim();
    
    if (email.length === 0) {
        hideAllErrorsForInput(loginEmailInput);
    } else if (!emailRegex.test(email)) {
        showError(loginEmailInput, "login-email-error", true);
        document.getElementById("login-email-error").textContent = "Please enter a valid email.";
    } else {
        hideAllErrorsForInput(loginEmailInput);
    }
});

loginPasswordInput.addEventListener('input', () => {
    const password = loginPasswordInput.value;
    
    if (password.length === 0) {
        hideAllErrorsForInput(loginPasswordInput);
    } else {
        hideAllErrorsForInput(loginPasswordInput);
    }
});

// ========== REAL-TIME VALIDATION - SIGNUP ==========
const signupNameInput = document.getElementById("signup-name");
const signupEmailInput = document.getElementById("signup-email");
const signupConfirmPasswordInput = document.getElementById("signup-confirm-password");

signupNameInput.addEventListener('focus', () => {
    hideAllErrorsForInput(signupNameInput);
});

signupEmailInput.addEventListener('focus', () => {
    hideAllErrorsForInput(signupEmailInput);
});

signupConfirmPasswordInput.addEventListener('focus', () => {
    hideAllErrorsForInput(signupConfirmPasswordInput);
});

signupNameInput.addEventListener('input', () => {
    const name = signupNameInput.value.trim();
    
    if (name.length === 0) {
        hideAllErrorsForInput(signupNameInput);
    } else {
        hideAllErrorsForInput(signupNameInput);
    }
});

signupEmailInput.addEventListener('input', () => {
    const email = signupEmailInput.value.trim();
    
    if (email.length === 0) {
        hideAllErrorsForInput(signupEmailInput);
    } else if (!emailRegex.test(email)) {
        showError(signupEmailInput, "signup-email-error", true);
        document.getElementById("signup-email-error").textContent = "Please enter a valid email.";
    } else {
        hideAllErrorsForInput(signupEmailInput);
    }
});

// Password validation with strength indicator
signupPasswordInput.addEventListener('input', () => {
    const password = signupPasswordInput.value;
    const confirmPassword = signupConfirmPasswordInput.value;
    
    if (password.length === 0) {
        hideAllErrorsForInput(signupPasswordInput);
        resetPasswordStrengthIndicators();
    } else {
        const isStrong = checkPasswordStrength(password);
        
        // Auto-hide dropdown when all requirements are met
        if (isStrong) {
            setTimeout(() => {
                if (document.activeElement !== signupPasswordInput) {
                    passwordStrengthDropdown.classList.add('hidden');
                }
            }, 1000);
        }
        
        hideAllErrorsForInput(signupPasswordInput);
    }
    
    // Also validate confirm password if it has content
    if (confirmPassword.length > 0) {
        if (password !== confirmPassword) {
            showError(signupConfirmPasswordInput, "signup-password-match-error", true);
        } else {
            hideAllErrorsForInput(signupConfirmPasswordInput);
        }
    }
});

signupConfirmPasswordInput.addEventListener('input', () => {
    const password = signupPasswordInput.value;
    const confirmPassword = signupConfirmPasswordInput.value;
    
    if (confirmPassword.length === 0) {
        hideAllErrorsForInput(signupConfirmPasswordInput);
    } else if (password !== confirmPassword) {
        showError(signupConfirmPasswordInput, "signup-password-match-error", true);
    } else {
        hideAllErrorsForInput(signupConfirmPasswordInput);
    }
});

// ========== REAL-TIME VALIDATION - FORGOT PASSWORD ==========
forgotEmailInput.addEventListener('focus', () => {
    hideAllErrorsForInput(forgotEmailInput);
});

forgotEmailInput.addEventListener('input', () => {
    const email = forgotEmailInput.value.trim();

    if (email.length === 0) {
        hideAllErrorsForInput(forgotEmailInput);
    } else if (!emailRegex.test(email)) {
        showError(forgotEmailInput, "forgot-email-error", true);
        document.getElementById("forgot-email-error").textContent = "Please enter a valid email.";
    } else {
        hideAllErrorsForInput(forgotEmailInput);
    }
});

forgotCodeInput.addEventListener('focus', () => {
    hideAllErrorsForInput(forgotCodeInput);
});

forgotCodeInput.addEventListener('input', () => {
    forgotCodeInput.value = forgotCodeInput.value.replace(/\D/g, "").slice(0, 6);

    if (forgotCodeInput.value.length === 0) {
        hideAllErrorsForInput(forgotCodeInput);
    } else if (!codeRegex.test(forgotCodeInput.value)) {
        showError(forgotCodeInput, "forgot-code-error", true);
    } else {
        hideAllErrorsForInput(forgotCodeInput);
    }
});

// ========== LOGIN FORM SUBMISSION ==========
loginForm.addEventListener("submit", (e) => {
    e.preventDefault();
    clearFormErrors("auth-login");

    const email = loginEmailInput.value.trim();
    const password = loginPasswordInput.value;
    let isValid = true;

    if (!email) {
        showError(loginEmailInput, "login-email-error", true);
        document.getElementById("login-email-error").textContent = "Please enter your email.";
        isValid = false;
    } else if (!emailRegex.test(email)) {
        showError(loginEmailInput, "login-email-error", true);
        document.getElementById("login-email-error").textContent = "Please enter a valid email.";
        isValid = false;
    }

    if (!password.trim()) {
        showError(loginPasswordInput, "login-password-error", true);
        isValid = false;
    }

    if (isValid) {
        console.log("Login successful:", { email });
        closeAuth();
    }
});

// ========== SIGNUP FORM SUBMISSION ==========
signupForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    clearFormErrors("auth-signup");

    const name = signupNameInput.value.trim();
    const email = signupEmailInput.value.trim();
    const password = signupPasswordInput.value;
    const confirmPassword = signupConfirmPasswordInput.value;
    let isValid = true;

    if (!name) {
        showError(signupNameInput, "signup-name-error", true);
        isValid = false;
    }

    if (!email) {
        showError(signupEmailInput, "signup-email-error", true);
        document.getElementById("signup-email-error").textContent = "Please enter your email.";
        isValid = false;
    } else if (!emailRegex.test(email)) {
        showError(signupEmailInput, "signup-email-error", true);
        document.getElementById("signup-email-error").textContent = "Please enter a valid email.";
        isValid = false;
    }

    if (!password.trim()) {
        showError(signupPasswordInput, "signup-password-error", true);
        isValid = false;
    } else if (!strongPasswordRegex.test(password)) {
        showError(signupPasswordInput, "signup-password-strength-error", true);
        isValid = false;
    }

    if (!confirmPassword.trim()) {
        showError(signupConfirmPasswordInput, "signup-confirm-password-error", true);
        isValid = false;
    } else if (password !== confirmPassword) {
        showError(signupConfirmPasswordInput, "signup-password-match-error", true);
        isValid = false;
    }

    if (!isValid) return;

    try {
        const response = await fetch("/api/auth/signup", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                username: name,
                email,
                password,
            }),
        });

        const payload = await response.json();
        if (!response.ok) {
            if (response.status === 409) {
                showError(signupEmailInput, "signup-email-error", true);
                document.getElementById("signup-email-error").textContent = payload.error || "An account with this email already exists.";
                return;
            }

            showError(signupEmailInput, "signup-email-error", true);
            document.getElementById("signup-email-error").textContent = payload.error || "Signup failed. Please try again.";
            return;
        }

        console.log("Signup successful:", payload.user);
        setAuthUiState({
            ...payload.user,
            email_verified: Boolean(payload.user?.email_verified),
        });
        closeAuth();
    } catch (error) {
        showError(signupEmailInput, "signup-email-error", true);
        document.getElementById("signup-email-error").textContent = "Network error. Please try again.";
    }
});

// ========== FORGOT PASSWORD FORM SUBMISSION ==========
forgotForm.addEventListener("submit", (e) => {
    e.preventDefault();
    clearFormErrors("auth-forgot");

    const email = forgotEmailInput.value.trim();
    const isCodeStepVisible = !forgotCodeStep.classList.contains("hidden");
    let isValid = true;

    if (!email) {
        showError(forgotEmailInput, "forgot-email-error", true);
        document.getElementById("forgot-email-error").textContent = "Please enter your email.";
        isValid = false;
    } else if (!emailRegex.test(email)) {
        showError(forgotEmailInput, "forgot-email-error", true);
        document.getElementById("forgot-email-error").textContent = "Please enter a valid email.";
        isValid = false;
    }

    if (!isValid) return;

    if (!isCodeStepVisible) {
        console.log("Forgot password code requested:", { email });
        showForgotPasswordCodeStep();
        return;
    }

    const code = forgotCodeInput.value.trim();
    if (!codeRegex.test(code)) {
        showError(forgotCodeInput, "forgot-code-error", true);
        isValid = false;
    }

    if (isValid) {
        console.log("Forgot password code verified:", { email, code });
        closeAuth();
    }
});

const SIDEBAR_STATE_KEY = "insighthub.sidebarCollapsed";
const CONVO_STATE_KEY = "insighthub.convoOpen";
const THEME_STATE_KEY = "insighthub.theme";

// Sidebar toggle
const sidebarToggle = document.getElementById("sidebar-toggle");

// Conversations submenu toggle
const convoToggle = document.getElementById("convo-toggle");
const convoSubmenu = document.getElementById("convo-submenu");
const convoCaret = document.getElementById("convo-caret");
const themeLightRadio = document.getElementById("theme-light");
const themeDarkRadio = document.getElementById("theme-dark");
const themeLightOption = document.getElementById("theme-light-option");
const themeDarkOption = document.getElementById("theme-dark-option");

const syncConvoCaret = () => {
    const isOpen = !convoSubmenu.classList.contains("hidden");
    convoCaret.classList.toggle("rotate-180", isOpen);
};

const syncThemeOptions = (theme) => {
    const isDark = theme === "dark";
    themeLightOption.classList.toggle("active", !isDark);
    themeDarkOption.classList.toggle("active", isDark);
    themeLightRadio.checked = !isDark;
    themeDarkRadio.checked = isDark;
};

const applyTheme = (theme, persist = true) => {
    const nextTheme = theme === "dark" ? "dark" : "light";
    document.body.classList.toggle("theme-dark", nextTheme === "dark");
    document.body.classList.toggle("theme-light", nextTheme !== "dark");
    syncThemeOptions(nextTheme);
    if (persist) {
        localStorage.setItem(THEME_STATE_KEY, nextTheme);
    }
};

// Restore persisted UI state on refresh
const isSidebarCollapsed = localStorage.getItem(SIDEBAR_STATE_KEY) === "true";
if (isSidebarCollapsed) {
    document.body.classList.add("sidebar-collapsed");
}

const savedConvoOpen = localStorage.getItem(CONVO_STATE_KEY);
if (savedConvoOpen === "false") {
    convoSubmenu.classList.add("hidden");
} else if (savedConvoOpen === "true") {
    convoSubmenu.classList.remove("hidden");
}

const savedTheme = localStorage.getItem(THEME_STATE_KEY);
applyTheme(savedTheme === "dark" ? "dark" : "light", false);
syncConvoCaret();

sidebarToggle.addEventListener("click", () => {
    document.body.classList.toggle("sidebar-collapsed");
    localStorage.setItem(
        SIDEBAR_STATE_KEY,
        document.body.classList.contains("sidebar-collapsed")
    );
});

convoToggle.addEventListener("click", () => {
    const isHidden = convoSubmenu.classList.toggle("hidden");
    localStorage.setItem(CONVO_STATE_KEY, (!isHidden).toString());
    syncConvoCaret();
});

themeLightRadio.addEventListener("change", () => {
    if (themeLightRadio.checked) applyTheme("light");
});
themeDarkRadio.addEventListener("change", () => {
    if (themeDarkRadio.checked) applyTheme("dark");
});

const serverAuthUser = window.__AUTH_USER__ || null;
setAuthUiState(serverAuthUser);
